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

#include "recovery_manager.hpp"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <mutex>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

namespace {

bool RecoveryTraceEnabled() {
  static const bool enabled = std::getenv("TINYLAMB_RECOVERY_TRACE") != nullptr;
  return enabled;
}

// Garbage bytes must fail parsing (and trigger tail truncation) instead of
// decoding into an undefined LogType that Release builds silently accept.
bool IsKnownLogType(LogType type) {
  switch (type) {
    case LogType::kUnknown:
      return false;
    case LogType::kBegin:
    case LogType::kInsertRow:
    case LogType::kInsertLeaf:
    case LogType::kInsertBranch:
    case LogType::kUpdateRow:
    case LogType::kUpdateLeaf:
    case LogType::kUpdateBranch:
    case LogType::kDeleteRow:
    case LogType::kDeleteLeaf:
    case LogType::kDeleteBranch:
    case LogType::kSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kSetFoster:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateDeleteBranch:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kLowestValue:
      return true;
  }
  return false;
}

bool IsPageManipulation(LogType type) {
  switch (type) {
    case LogType::kUnknown:
      throw std::runtime_error("Invalid format log");

    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      return false;

    default:
      return true;
  }
}

void LogRedo(PageRef& target, lsn_t lsn, const LogRecord& log) {
  // The apply/no-apply decision (page_lsn vs record LSN) belongs to
  // PageReplay, which is this function's only caller. In particular a
  // kSystemAllocPage at LSN 0 on a freshly recovered page must run even
  // though page_lsn also reads 0 there.
  if (!IsPageManipulation(log.type)) {
    return;
  }

  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kInsertRow:
    case LogType::kCompensateDeleteRow:
      target->InsertImpl(log.slot, log.redo_data);
      break;
    case LogType::kUpdateRow:
    case LogType::kCompensateUpdateRow:
      target->UpdateImpl(log.slot, log.redo_data);
      break;
    case LogType::kDeleteRow:
    case LogType::kCompensateInsertRow:
      target->DeleteImpl(log.slot);
      break;
    case LogType::kUpdateLeaf:
      target->UpdateImpl(log.key, log.redo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kCompensateInsertLeaf:
      target->DeleteImpl(log.key);
      break;
    case LogType::kDeleteBranch:
    case LogType::kCompensateInsertBranch:
      target->DeleteBranchImpl(log.key);
      break;
    case LogType::kCompensateUpdateLeaf:
      target->UpdateImpl(log.key, log.redo_data);
      break;
    case LogType::kInsertLeaf:
    case LogType::kCompensateDeleteLeaf:
      target->InsertImpl(log.key, log.redo_data);
      break;
    case LogType::kInsertBranch:
    case LogType::kCompensateDeleteBranch:
      target->InsertBranchImpl(log.key, log.redo_page);
      break;
    case LogType::kUpdateBranch:
    case LogType::kCompensateUpdateBranch:
      target->UpdateBranchImpl(log.key, log.redo_page);
      break;
    case LogType::kLowestValue:
      target->SetLowestValueBranchImpl(log.redo_page);
      break;
    case LogType::kSetFoster:
    case LogType::kCompensateSetFoster: {
      auto new_foster = Decode<FosterPair>(log.redo_data);
      target->SetFosterImpl(new_foster);
      break;
    }
    case LogType::kSystemAllocPage:
      target->PageInit(log.pid, log.allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      // Destroy-redo needs allocator semantics (free-list update) that the
      // WAL record does not carry: it has neither the old page image nor
      // the successor state of meta_page. Replaying it naively would
      // corrupt the page allocator, so fail loudly instead of guessing.
      // DESIGN DECISION REQUIRED -- see recovery/CODE_REVIEW.md.
      throw std::runtime_error(
          "kSystemDestroyPage redo not implemented (page " +
          std::to_string(log.pid) + ")");
    case LogType::kSetLowFence:
    case LogType::kCompensateSetLowFence: {
      auto ik = Decode<IndexKey>(log.redo_data);
      target->SetLowFenceImpl(ik);
      break;
    }
    case LogType::kSetHighFence:
    case LogType::kCompensateSetHighFence: {
      auto ik = Decode<IndexKey>(log.redo_data);
      target->SetHighFenceImpl(ik);
      break;
    }
    default:
      assert(!"must not reach here");
  }
  target->SetPageLSN(lsn);
}

void LogUndo(PageRef& target, lsn_t lsn, const LogRecord& log,
             TransactionManager* tm) {
  switch (log.type) {
    case LogType::kUnknown:
      LOG(FATAL) << "Unknown type log";
      throw std::runtime_error("broken log");
    case LogType::kInsertRow:
      tm->CompensateInsertLog(log.txn_id, log.pid, log.slot);
      target->DeleteImpl(log.slot);
      break;
    case LogType::kUpdateRow:
      tm->CompensateUpdateLog(log.txn_id, log.pid, log.slot, log.undo_data);
      target->UpdateImpl(log.slot, log.undo_data);
      break;
    case LogType::kDeleteRow:
      tm->CompensateDeleteLog(log.txn_id, log.pid, log.slot, log.undo_data);
      target->InsertImpl(log.slot, log.undo_data);
      break;
    case LogType::kSystemDestroyPage:
      // Undoing a destroy cannot restore the lost page image: the record
      // carries neither the old content nor the original page type
      // (allocated_page_type is kUnknown for destroy records). The
      // reinitialization below is lossy but keeps the page id addressable;
      // pinned by AbortWithDestroyPageLogReinitializesPage.
      // DESIGN DECISION REQUIRED -- see recovery/CODE_REVIEW.md.
      target->PageInit(log.pid, log.allocated_page_type);
      break;
    case LogType::kInsertLeaf:
      tm->CompensateInsertLog(log.txn_id, log.pid, log.key);
      target->DeleteImpl(log.key);
      break;
    case LogType::kInsertBranch:
      tm->CompensateInsertBranchLog(log.txn_id, log.pid, log.key);
      target->DeleteBranchImpl(log.key);
      break;
    case LogType::kUpdateLeaf:
      tm->CompensateUpdateLog(log.txn_id, log.pid, log.key, log.undo_data);
      target->UpdateImpl(log.key, log.undo_data);
      break;
    case LogType::kUpdateBranch:
      tm->CompensateUpdateBranchLog(log.txn_id, log.pid, log.key,
                                    log.undo_page);
      target->UpdateBranchImpl(log.key, log.undo_page);
      break;
    case LogType::kDeleteLeaf:
      tm->CompensateDeleteLog(log.txn_id, log.pid, log.key, log.undo_data);
      target->InsertImpl(log.key, log.undo_data);
      break;
    case LogType::kDeleteBranch:
      tm->CompensateDeleteBranchLog(log.txn_id, log.pid, log.key,
                                    log.undo_page);
      target->InsertBranchImpl(log.key, log.undo_page);
      break;
    case LogType::kLowestValue: {
      tm->CompensateSetLowestValueLog(log.txn_id, log.pid, log.undo_page);
      target->SetLowestValueBranchImpl(log.undo_page);
      break;
    }
    case LogType::kSetLowFence: {
      auto undo_key = Decode<IndexKey>(log.undo_data);
      tm->CompensateSetLowFenceLog(log.txn_id, log.pid, undo_key);
      target->SetLowFenceImpl(undo_key);
      break;
    }
    case LogType::kSetHighFence: {
      auto undo_key = Decode<IndexKey>(log.undo_data);
      tm->CompensateSetHighFenceLog(log.txn_id, log.pid, undo_key);
      target->SetHighFenceImpl(undo_key);
      break;
    }
    case LogType::kSetFoster: {
      auto foster = Decode<FosterPair>(log.undo_data);
      tm->CompensateSetFosterLog(log.txn_id, log.pid, foster);
      target->SetFosterImpl(foster);
      break;
    }
    case LogType::kSystemAllocPage:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateDeleteBranch:
    case LogType::kCompensateSetLowFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kCompensateSetFoster:
      // Compensating log cannot undo.
      break;
  }
  target->SetPageLSN(lsn);
}

// Precondition: the page is locked by this thread.
void PageReplay(PageRef&& target,
                const std::vector<std::pair<lsn_t, LogRecord>>& logs,
                const std::unordered_set<txn_id_t>& committed_txn,
                TransactionManager* tm,
                RecoveryManager::UndoneRecorder* undone) {
  // Redo & Undo a specific page.

  // Redo phase.
  for (const auto& lsn_log : logs) {
    const LogRecord& log = lsn_log.second;
    assert(log.pid == target->PageID());

    const lsn_t& lsn = lsn_log.first;
    // A freshly recovered (broken) page starts at page_lsn == 0 meaning
    // *nothing* has been applied -- including a record that itself sits at
    // LSN 0. Since the kBegin removal the very first WAL record can be the
    // idempotent kSystemAllocPage at LSN 0, so treat "applied == 0" as
    // "nothing applied yet". Flushed images with page_lsn == 0 can only
    // contain such an ALLOCATE, whose redo (PageInit) is a no-op on them.
    const bool nothing_applied_yet = target->PageLSN() == 0;
    if (nothing_applied_yet || target->PageLSN() < lsn) {
      if (RecoveryTraceEnabled()) { LOG(INFO) << "redo: " << log;
}
      LogRedo(target, lsn, log);
    }
  }

  // Undo phase.
  for (const auto& log : std::ranges::reverse_view(logs)) {
    const LogRecord& undo_log = log.second;
    const auto it = committed_txn.find(undo_log.txn_id);
    assert(undo_log.pid == target->PageID());
    if (it == committed_txn.end()) {
      if (RecoveryTraceEnabled()) { LOG(INFO) << "undo: " << undo_log;
}
      LogUndo(target, log.first, undo_log, tm);
      if (undone != nullptr) { undone->Record(log.first);
}
    }
  }

  // Release the page latch.
  if (RecoveryTraceEnabled()) {
    LOG(INFO) << "SPR " << target->PageID() << " finished";
  }
  target.PageUnlock();
}

size_t RecoveryWorkerCount(size_t jobs) {
  if (jobs <= 1) { return jobs;
}
  size_t workers = std::thread::hardware_concurrency();
  if (workers == 0) { workers = 1;
}
  if (const char* env = std::getenv("TINYLAMB_RECOVERY_WORKERS");
      env != nullptr && env[0] != '\0') {
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(env, &end, 10);
    // Accept only a fully numeric value; "12abc" or garbage keeps the
    // hardware default instead of silently adopting a partial parse.
    if (end != env && *end == '\0' && parsed > 0) { workers = parsed;
}
  }
  return std::min(jobs, workers);
}

void ReplayPagesInParallel(
    PagePool* pool,
    std::vector<
        std::pair<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>>* jobs,
    const std::unordered_set<txn_id_t>& committed_txn, TransactionManager* tm,
    RecoveryManager::UndoneRecorder* undone) {
  if (jobs->empty()) { return;
}
  const size_t workers = RecoveryWorkerCount(jobs->size());
  auto replay_one = [&](size_t index) {
    auto& [page_id, logs] = (*jobs)[index];
    // Lock and unlock on this worker thread. Moving a PageRef (and its
    // unique_lock) across threads is undefined for std::shared_mutex.
    PageRef page = pool->GetPage(page_id, nullptr);
    PageReplay(std::move(page), logs, committed_txn, tm, undone);
  };
  if (workers <= 1) {
    for (size_t i = 0; i < jobs->size(); ++i) { replay_one(i);
}
    return;
  }

  std::atomic<size_t> next{0};
  std::mutex error_mutex;
  std::exception_ptr first_error;
  auto worker = [&]() {
    try {
      for (;;) {
        const size_t index = next.fetch_add(1, std::memory_order_relaxed);
        if (index >= jobs->size()) { return;
}
        replay_one(index);
      }
    } catch (...) {
      std::scoped_lock lock(error_mutex);
      if (!first_error) { first_error = std::current_exception();
}
      next.store(jobs->size(), std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(workers);
  for (size_t i = 0; i < workers; ++i) {
    threads.emplace_back(worker);
  }
  for (std::thread& thread : threads) { thread.join();
}
  if (first_error) { std::rethrow_exception(first_error);
}
}

}  // namespace

namespace {

// file_size() that reports a missing/unreadable log as empty instead of
// throwing filesystem_error out of recovery. A brand-new database may reach
// here before any log byte exists.
std::uintmax_t LogFileSizeOrZero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  return ec ? 0 : size;
}

}  // namespace

RecoveryManager::RecoveryManager(std::string_view log_path, PagePool* pp)
    : log_name_(log_path), pool_(pp) {
  std::ignore = OpenReadFd();
}

RecoveryManager::~RecoveryManager() {
  if (read_fd_ >= 0) {
    ::close(read_fd_);
    read_fd_ = -1;
  }
}

bool RecoveryManager::OpenReadFd() const {
  if (read_fd_ >= 0) {
    return true;
  }
  // O_RDONLY pread-only handle: concurrent readers never share file state,
  // so parallel Abort() walks of the prev_lsn chains are race-free.
  read_fd_ = ::open(std::string(log_name_).c_str(), O_RDONLY | O_CLOEXEC);
  return read_fd_ >= 0;
}

void RecoveryManager::SinglePageRecovery(PageRef&& page,
                                         TransactionManager* tm,
                                         UndoneRecorder* undone) {
  SinglePageRecovery(std::move(page), tm, undone, LogFileSizeOrZero(log_name_));
}

void RecoveryManager::SinglePageRecovery(PageRef&& page, TransactionManager* tm,
                                         UndoneRecorder* undone,
                                         std::uintmax_t scan_end) {
  // Collects all logs to redo & undo for each page. RecoverFrom truncates a
  // torn tail beforehand; this break is only a standalone-call safety net.
  const std::uintmax_t filesize =
      std::min<std::uintmax_t>(LogFileSizeOrZero(log_name_), scan_end);
  std::vector<std::pair<lsn_t, LogRecord>> page_logs;
  std::unordered_set<txn_id_t> committed_txn;
  {
    lsn_t offset = 0;
    LogRecord log;
    while (offset < filesize) {
      bool success = ReadLog(offset, &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type) && log.pid == page->PageID()) {
        page_logs.emplace_back(offset, log);
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  PageReplay(std::move(page), page_logs, committed_txn, tm, undone);
}

void RecoveryManager::RecoverFrom(lsn_t checkpoint_lsn,
                                  TransactionManager* tm) {
  const std::uintmax_t on_disk = LogFileSizeOrZero(log_name_);
  if (on_disk == 0) {
    // Missing or empty log (e.g. first boot of a fresh database): nothing
    // to replay.
    LOG(INFO) << "Log file absent or empty, skipping recovery: " << log_name_;
    return;
  }

  // Torn tail policy (single authority, section 1.9): the first unparseable
  // offset ends the log; everything past it is a half-written append from the
  // crash and is truncated here so all later passes share one log end.
  UndoneRecorder undone;
  const lsn_t valid_end = ValidLogEnd(checkpoint_lsn);
  if (valid_end < static_cast<lsn_t>(on_disk)) {
    if (!TornTailTruncationAllowed()) {
      // Default policy: WAL corruption is unrecoverable damage. Say where
      // the log went bad, then die -- replaying a truncated prefix without
      // the operator's consent could silently lose committed transactions.
      LOG(FATAL) << "WAL corruption at offset " << valid_end << " of "
                 << log_name_ << " (" << (on_disk - valid_end)
                 << " unparsable bytes). Restart with --force to truncate "
                 << "the damaged tail and recover its intact prefix.";
    }
    if (read_fd_ < 0) {
      LOG(ERROR) << "Log file unreadable, skipping recovery";
      return;
    }
    LOG(INFO) << "Truncating torn log tail: " << on_disk << " -> " << valid_end;
    std::filesystem::resize_file(log_name_, valid_end);
  }
  const std::uintmax_t filesize = std::min<std::uintmax_t>(on_disk, valid_end);

  // Analysis phase starts here.
  std::unordered_map<page_id_t, lsn_t> dirty_page_table;

  auto UpdateOldestLSN = [&](page_id_t pid, lsn_t maybe_oldest) {
    const auto it = dirty_page_table.find(pid);
    if (it == dirty_page_table.end()) {
      dirty_page_table.emplace(pid, maybe_oldest);
    } else {
      dirty_page_table[it->first] = std::min(maybe_oldest, it->second);
    }
  };

  std::unordered_set<txn_id_t> committed_txn;
  // Newest LSN per transaction seen in the scanned region; feeds loser chain
  // heads for the global UNDO pass.
  std::unordered_map<txn_id_t, lsn_t> txn_last_lsn;
  // last_lsn of transactions still running at their latest EndCheckpoint.
  std::unordered_map<txn_id_t, lsn_t> att_running_lsn;
  lsn_t redo_start_point = valid_end;
  // Highest page id the WAL ever touched. The MetaPage mutates its allocator
  // state (max_page_count) outside the WAL, so a crash can lose that counter
  // while per-page records survive; without re-deriving it here the allocator
  // would re-issue live page ids and a later AllocateNewPage would wipe one.
  page_id_t max_seen_pid = 0;
  {
    lsn_t offset = checkpoint_lsn;
    LogRecord log;
    while (offset < filesize) {
      bool success = ReadLog(offset, &log);
      if (!success) {
        // Unreachable after tail normalization; keep the scan bounded.
        break;
      }
      if (RecoveryTraceEnabled()) {
        LOG(TRACE) << "analyzing: " << offset << ": " << log
                   << "  to: " << offset + log.Size();
      }
      if (log.type != LogType::kEndCheckpoint &&
          log.type != LogType::kBeginCheckpoint) {
        txn_last_lsn[log.txn_id] = offset;
      }
      if (IsPageManipulation(log.type)) {
        // Collect the oldest LSN to dirty_page_table.
        UpdateOldestLSN(log.pid, offset);
        max_seen_pid = std::max(max_seen_pid, log.pid);
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      } else if (log.type == LogType::kEndCheckpoint) {
        // Collect the oldest LSN to dirty_page_table.
        for (const auto& dpt : log.dirty_page_table) {
          UpdateOldestLSN(dpt.first, dpt.second);
          // PRODUCTION FIX: pages allocated BEFORE the checkpoint appear only
          // in its dirty-page table (analysis starts at checkpoint_lsn).  Not
          // folding them into max_seen_pid re-issued live page ids after a
          // crash when the allocator high-water mark was lost.
          max_seen_pid = std::max(max_seen_pid, dpt.first);
        }
        for (const auto& at : log.active_transaction_table) {
          if (at.status == TransactionStatus::kCommitted) {
            committed_txn.insert(at.txn_id);
          } else if (at.status == TransactionStatus::kRunning &&
                     at.last_lsn != 0) {
            att_running_lsn[at.txn_id] = at.last_lsn;
          }
        }
      }
      offset += log.Size();
    }

    // Take minimum LSN from the dirty page table.
    for (const auto& d : dirty_page_table) {
      redo_start_point = std::min(redo_start_point, d.second);
    }
  }

  // Loser chain heads: newest LSN of every transaction without a commit
  // record, from both the scanned region and the checkpoint's active
  // transaction table (covers losers whose writes precede checkpoint_lsn).
  std::vector<lsn_t> loser_heads;
  for (const auto& [txn, lsn] : txn_last_lsn) {
    if (!committed_txn.contains(txn)) {
      loser_heads.push_back(lsn);
    }
  }
  for (const auto& [txn, lsn] : att_running_lsn) {
    if (!committed_txn.contains(txn)) {
      const auto it = txn_last_lsn.find(txn);
      if (it == txn_last_lsn.end() || it->second <= lsn) {
        loser_heads.push_back(lsn);
      }
    }
  }
  std::ranges::sort(loser_heads, std::greater<>());

  // Collect all page references.
  std::unordered_map<page_id_t, PageRef> pages;
  pages.reserve(dirty_page_table.size());

  // Take all dirty page's lock.
  for (const auto& it : dirty_page_table) {
    // A broken on-disk image must reach the SPR check below verbatim, so this
    // loop bypasses PagePool checksum enforcement.
    PageRef&& page = pool_->GetPageForRecovery(it.first, nullptr);
    if (!page->IsValid()) {
      page->page_lsn = 0;
      page->page_id = it.first;
      LOG(INFO) << "Page " << it.first << " is broken, start SPR.";
      SinglePageRecovery(std::move(page), tm, &undone, valid_end);
    } else {
      pages.emplace(it.first, std::move(page));
    }
  }
  // Now other user transactions can start concurrently.

  // Collects all logs to redo & undo for each page.
  std::unordered_map<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>
      page_logs;
  page_logs.reserve(pages.size());
  {
    lsn_t offset = redo_start_point;
    LogRecord log;
    while (offset < filesize) {
      bool success = ReadLog(offset, &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type)) {
        const auto it = pages.find(log.pid);
        if (it != pages.end()) {
          page_logs[log.pid].emplace_back(offset, log);
        }
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  // Redo & Undo phase: Instant Recovery groups logs by page_id. Each dirty
  // page is exclusively latched above so concurrent user transactions cannot
  // touch them during analysis. Release those PageRefs on this thread (mutex
  // unlock must stay on the locking thread), then re-acquire inside workers.
  std::vector<std::pair<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>>
      replay_jobs;
  replay_jobs.reserve(page_logs.size());
  for (auto& [page_id, logs] : page_logs) {
    replay_jobs.emplace_back(page_id, std::move(logs));
  }
  pages.clear();
  ReplayPagesInParallel(pool_, &replay_jobs, committed_txn, tm, &undone);

  // Global loser UNDO (section 1.7): compensate every record on each loser's
  // prev_lsn chain that per-page replay could not see -- pages outside the
  // dirty page table or records below redo_start_point.
  UndoLoserChains(loser_heads, &undone, tm, valid_end);

  // Restore the MetaPage allocator high-water mark. AllocateNewPage bumps
  // max_page_count in place without a dedicated WAL record, so a crash right
  // after an allocation can lose the counter while per-page records survive;
  // re-deriving it from every touched page id prevents the allocator from
  // re-issuing live page ids (which would let a later AllocateNewPage
  // PageInit over an existing tree root). Freed pages may leak (never
  // reused) but are never handed out twice.
  PageRef meta = pool_->GetPageForRecovery(kMetaPageId, nullptr);
  if (!meta->IsValid()) {
    meta->page_id = kMetaPageId;
    meta->page_lsn = 0;
    meta->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  auto& allocator = meta->body.meta_page;
  if (allocator.MaxPageCount() < max_seen_pid) {
    LOG(INFO) << "Restoring meta allocator high-water mark: "
              << allocator.MaxPageCount() << " -> " << max_seen_pid;
    allocator.RestoreMaxPageCount(max_seen_pid);
  }
}

lsn_t RecoveryManager::ValidLogEnd(lsn_t from) const {
  const std::uintmax_t filesize = LogFileSizeOrZero(log_name_);
  lsn_t offset = from;
  LogRecord log;
  while (offset < filesize && ReadLog(offset, &log)) {
    offset += log.Size();
  }
  return offset;
}

bool RecoveryManager::ReadLog(lsn_t lsn, LogRecord* dst) const {
  dst->Clear();
  if (read_fd_ < 0 && !OpenReadFd()) {
    return false;
  }
  // Validate the record type from a private pread snapshot BEFORE decoding:
  // garbage bytes must fail here instead of reaching LogRecord's decoder
  // switch, which asserts on unknown types in debug builds.
  std::array<char, sizeof(uint32_t) * 2 + sizeof(uint16_t)> header{};
  ssize_t nhead = 0;
  do {
    nhead = ::pread(read_fd_, header.data(), header.size(),
                    static_cast<off_t>(lsn));
  } while (nhead < 0 && errno == EINTR);
  uint32_t magic = 0;
  uint32_t version = 0;
  uint16_t raw_type = 0;
  if (!std::cmp_less(nhead, header.size())) {
    DeserializeU32(header.data(), &magic);
    DeserializeU32(header.data() + sizeof(uint32_t), &version);
    DeserializeU16(header.data() + sizeof(uint32_t) * 2, &raw_type);
  }
  if (std::cmp_less(nhead, header.size()) || magic != kSerdesMagic ||
      version != kSerdesVersion ||
      !IsKnownLogType(static_cast<LogType>(raw_type))) {
    return false;
  }
  // Records are variable length; read a window via pread into a private
  // buffer and grow it until the record decodes or EOF proves it torn. The
  // fd is never seeked or shared, so concurrent calls are independent.
  constexpr size_t kInitialWindow = 4096;
  constexpr size_t kMaxWindow = 16U << 20;
  for (size_t want = kInitialWindow;; want *= 4) {
    std::string buffer(want, '\0');
    ssize_t nread = 0;
    do {
      nread = ::pread(read_fd_, buffer.data(), want,
                      static_cast<off_t>(lsn));
    } while (nread < 0 && errno == EINTR);
    if (nread <= 0) {
      return false;
    }
    buffer.resize(static_cast<size_t>(nread));
    std::istringstream in;
    in.str(std::move(buffer));
    Decoder dec(in);
    dec >> *dst;
    if (in.good() && IsKnownLogType(dst->type)) {
      return true;
    }
    if (std::cmp_less(nread, want) || want >= kMaxWindow) {
      return false;
    }
  }
}

void RecoveryManager::UndoLoserChains(const std::vector<lsn_t>& loser_heads,
                                      UndoneRecorder* undone,
                                      TransactionManager* tm,
                                      lsn_t scan_end) {
  for (lsn_t head : loser_heads) {
    std::unordered_set<lsn_t> visited;
    for (lsn_t cur = head; cur != 0;) {
      if (!visited.insert(cur).second) {
        break;  // Corrupted prev_lsn cycle guard.
      }
      LogRecord log;
      if (!ReadLog(cur, &log)) {
        break;
      }
      if (IsPageManipulation(log.type) && !undone->Contains(cur)) {
        PageRef target = pool_->GetPageForRecovery(log.pid, nullptr);
        if (!target->IsValid()) {
          LOG(INFO) << "Loser undo rebuilds broken page " << log.pid;
          SinglePageRecovery(std::move(target), tm, undone, scan_end);
        } else if (target->PageLSN() >= cur) {
          // Only revert changes the page image actually reflects; older
          // images never received this change (and the next recovery cycle
          // redoes+undoes it deterministically).
          LogUndo(target, cur, log, tm);
          undone->Record(cur);
        }
      }
      cur = log.prev_lsn;
    }
  }
}

void RecoveryManager::LogUndoWithPage(lsn_t lsn, const LogRecord& log,
                                      TransactionManager* tm) {
  if (IsPageManipulation(log.type)) {
    PageRef target = pool_->GetPage(log.pid);
    LogUndo(target, lsn, log, tm);
  }
}

}  // namespace tinylamb
