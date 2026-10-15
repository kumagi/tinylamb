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
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
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
#include "common/crc32c.hpp"
#include "common/decoder.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

namespace {

bool RecoveryTraceEnabled() {
  static const bool enabled = std::getenv("TINYLAMB_RECOVERY_TRACE") != nullptr;
  return enabled;
}

// Rate-limited progress reporter for the multi-pass WAL scans. A
// multi-gigabyte retained log can keep a restart silent for minutes, and an
// operator tailing the boot log cannot tell forward progress from a hang.
// Tick() logs at most one line per interval; safe to call from the parallel
// sweep workers.
class RecoveryProgressLogger {
 public:
  RecoveryProgressLogger(std::string phase, lsn_t begin, lsn_t end)
      : phase_(std::move(phase)), begin_(begin), end_(end) {}

  void Tick(lsn_t at) {
    const auto now = std::chrono::steady_clock::now();
    std::scoped_lock lock(mu_);
    if (now - last_ < kInterval) {
      return;
    }
    last_ = now;
    const lsn_t span = end_ > begin_ ? end_ - begin_ : 1;
    const lsn_t done = std::min(std::max(at, begin_), end_) - begin_;
    LOG(INFO) << "recovery " << phase_ << ": position " << at << " ("
              << (done * 100) / span << "%)";
  }

 private:
  static constexpr std::chrono::milliseconds kInterval{2'000};
  std::string phase_;
  lsn_t begin_;
  lsn_t end_;
  mutable std::mutex mu_;
  // Aligned with now: the first Tick logs only after a full interval of
  // scanning, so a small WAL's recovery stays silent instead of emitting a
  // pair of 0% lines.
  std::chrono::steady_clock::time_point last_ =
      std::chrono::steady_clock::now();
};

// Garbage bytes must fail parsing (and trigger tail truncation) instead of
// decoding into an undefined LogType that Release builds silently accept.
// A kCompensateX record proves that an earlier recovery pass already undid
// an operation of type X (never X itself, and only at a LOWER LSN). Maps a
// compensating record back to the operation type it closes.
std::optional<LogType> CompensatedOpType(LogType type) {
  switch (type) {
    case LogType::kCompensateInsertRow:
      return LogType::kInsertRow;
    case LogType::kCompensateInsertLeaf:
      return LogType::kInsertLeaf;
    case LogType::kCompensateInsertBranch:
      return LogType::kInsertBranch;
    case LogType::kCompensateUpdateRow:
      return LogType::kUpdateRow;
    case LogType::kCompensateUpdateLeaf:
      return LogType::kUpdateLeaf;
    case LogType::kCompensateUpdateBranch:
      return LogType::kUpdateBranch;
    case LogType::kCompensateDeleteRow:
      return LogType::kDeleteRow;
    case LogType::kCompensateDeleteLeaf:
      return LogType::kDeleteLeaf;
    case LogType::kCompensateDeleteBranch:
      return LogType::kDeleteBranch;
    case LogType::kCompensateSetLowFence:
      return LogType::kSetLowFence;
    case LogType::kCompensateSetHighFence:
      return LogType::kSetHighFence;
    case LogType::kCompensateSetFoster:
      return LogType::kSetFoster;
    case LogType::kCompensateSetLowestValue:
      return LogType::kLowestValue;
    case LogType::kCompensateSetRowPageNext:
      return LogType::kSetRowPageNext;
    case LogType::kCompensateSetRowPagePrev:
      return LogType::kSetRowPagePrev;
    case LogType::kCompensateDestroyPage:
      return LogType::kSystemDestroyPage;
    case LogType::kCompensateAllocPage:
      return LogType::kSystemAllocPage;
    default:
      return std::nullopt;
  }
}

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
    case LogType::kCompensateSetLowestValue:
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
    case LogType::kCompensateDestroyPage:
    case LogType::kCompensateAllocPage:
    case LogType::kFullPageImage:
    case LogType::kLowestValue:
      return true;
  }
  return false;
}

bool IsPageManipulation(LogType type) {
  switch (type) {
    case LogType::kUnknown:
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      return false;

    default:
      return true;
  }
}

// True when the page still holds at least one live entry (rows, separators,
// fences into a foster child). A page-free that runs while this is true would
// strand the entries: entries a COMMITTED transaction inserted after the
// loser's last physical touch on the page are covered by no loser operation,
// so the rollback's collapse cannot move them out of the way.
bool HasLiveEntries(const PageRef& page) {
  switch (page->Type()) {
    case PageType::kLeafPage: {
      if (page->body.leaf_page.RowCount() != 0) {
        return true;
      }
      const StatusOr<FosterPair> foster = page->body.leaf_page.GetFoster();
      return foster.HasValue() && !foster.Value().IsEmpty();
    }
    case PageType::kBranchPage:
      return page->body.branch_page.RowCount() != 0;
    case PageType::kRowPage:
      return page->body.row_page.RowCount() != 0;
    default:
      return false;
  }
}

Status ValidateReplayPageType(const Page& target, LogType type) {
  const auto require = [&](PageType expected) -> Status {
    if (target.Type() == expected) {
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kCorrupt,
                       "WAL operation page type does not match target");
  };
  switch (type) {
    case LogType::kInsertRow:
    case LogType::kUpdateRow:
    case LogType::kDeleteRow:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kSetRowPageNext:
    case LogType::kSetRowPagePrev:
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
      return require(PageType::kRowPage);
    case LogType::kInsertLeaf:
    case LogType::kUpdateLeaf:
    case LogType::kDeleteLeaf:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateDeleteLeaf:
      return require(PageType::kLeafPage);
    case LogType::kInsertBranch:
    case LogType::kUpdateBranch:
    case LogType::kDeleteBranch:
    case LogType::kCompensateInsertBranch:
    case LogType::kCompensateUpdateBranch:
    case LogType::kCompensateDeleteBranch:
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
      return require(PageType::kBranchPage);
    case LogType::kSetLowFence:
    case LogType::kCompensateSetLowFence:
    case LogType::kSetHighFence:
    case LogType::kCompensateSetHighFence:
    case LogType::kSetFoster:
    case LogType::kCompensateSetFoster:
      if (target.Type() == PageType::kLeafPage ||
          target.Type() == PageType::kBranchPage) {
        return Status::kSuccess;
      }
      return StatusError(StatusCode::kCorrupt,
                         "WAL fence operation page type does not match target");
    default:
      return Status::kSuccess;
  }
}

// A CLR whose redo RE-INSERTS an undo-restored entry can outrun its page:
// a multi-pass recovery's torn-page rebuild anchors at a body-restoring
// record, so the forward deletes that once freed the entry's bytes can sit
// below the anchor while their compensating re-inserts replay above it --
// and the page between those LSNs is genuinely full. Failing the open would
// wedge the database forever on WAL that is already baked in; the skip is
// the same bounded-loss availability floor LogUndo's re-insert path takes
// (one loser-era entry stays absent, loudly warned, replays stay idempotent
// because the stamp advances either way). Forward (committed) inserts never
// take this path.
Status RedoCompensateReinsert(lsn_t lsn, const LogRecord& log,
                              const std::function<Status()>& reinsert) {
  Status status = reinsert();
  if (status.ok()) {
    return Status::kSuccess;
  }
  if (status == Status::kNoSpace || status == Status::kTooBigData) {
    if (std::getenv("TINYLAMB_DISABLE_REDO_REINSERT_FLOOR") != nullptr) {
      return status;
    }
    LOG(WARN) << "redo skips compensation re-insert (page full; the deletes "
                 "that freed its bytes sit below the rebuild anchor): lsn "
              << lsn << " page " << log.pid << " (" << log.type << ")";
    return Status::kSuccess;
  }
  return status;
}

Status LogRedo(PageRef& target, lsn_t lsn, const LogRecord& log) {
  // The apply/no-apply decision (page_lsn vs record LSN) belongs to
  // PageReplay, which is this function's only caller. In particular a
  // kSystemAllocPage at LSN 0 on a freshly recovered page must run even
  // though page_lsn also reads 0 there.
  if (log.type == LogType::kUnknown) {
    return StatusError(StatusCode::kCorrupt, "unknown log type");
  }
  const Status validate = ValidateReplayPageType(*target, log.type);
  if (validate != Status::kSuccess) {
    if (IsCompensation(log.type)) {
      // Twin of LogUndo's stale-incarnation skip: this CLR compensates an
      // operation whose page was destroyed or re-issued after the CLR was
      // written (a failed recovery pass already logged it). The physical
      // half is a no-op -- applying it would corrupt the newer incarnation
      // -- but the replay must not fail closed on it either, or the restart
      // wedges forever on its own earlier undo. Falling through returns
      // success; the page stamps below still advance, so re-replays stay
      // idempotent.
      LOG(WARN) << "redo skips physically stale compensation (page "
                   "incarnation changed): lsn "
                << lsn << " page " << target->PageID() << " (" << log.type
                << ")";
      return Status::kSuccess;
    }
    LOG(ERROR) << "replay type check failed at lsn " << lsn << " page "
               << target->PageID() << " (page_lsn " << target->PageLSN()
               << "): " << log;
  }
  RETURN_IF_FAIL(validate);
  if (!IsPageManipulation(log.type)) {
    return Status::kSuccess;
  }

  switch (log.type) {
    case LogType::kUnknown:
      return StatusError(StatusCode::kCorrupt, "unknown log type");
    case LogType::kInsertRow:
    case LogType::kCompensateDeleteRow:
      if (log.type == LogType::kCompensateDeleteRow) {
        RETURN_IF_FAIL(RedoCompensateReinsert(lsn, log, [&] {
          return target->InsertImpl(log.slot, log.redo_data);
        }));
      } else {
        RETURN_IF_FAIL(target->InsertImpl(log.slot, log.redo_data));
      }
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
      if (log.type == LogType::kCompensateDeleteLeaf) {
        RETURN_IF_FAIL(RedoCompensateReinsert(lsn, log, [&] {
          return target->InsertImpl(log.key, log.redo_data);
        }));
      } else {
        RETURN_IF_FAIL(target->InsertImpl(log.key, log.redo_data));
      }
      break;
    case LogType::kInsertBranch:
      RETURN_IF_FAIL(target->InsertBranchImpl(log.key, log.redo_page));
      break;
    case LogType::kCompensateDeleteBranch:
      RETURN_IF_FAIL(RedoCompensateReinsert(lsn, log, [&] {
        return target->InsertBranchImpl(log.key, log.redo_page);
      }));
      break;
    case LogType::kUpdateBranch:
    case LogType::kCompensateUpdateBranch:
      target->UpdateBranchImpl(log.key, log.redo_page);
      break;
    case LogType::kLowestValue:
    case LogType::kCompensateSetLowestValue:
      target->SetLowestValueBranchImpl(log.redo_page);
      break;
    case LogType::kSetRowPageNext:
    case LogType::kCompensateSetRowPageNext:
      target->SetNextPageImpl(log.redo_page);
      break;
    case LogType::kSetRowPagePrev:
    case LogType::kCompensateSetRowPagePrev:
      target->SetPrevPageImpl(log.redo_page);
      break;
    case LogType::kSetFoster:
    case LogType::kCompensateSetFoster: {
      ASSIGN_OR_RETURN(FosterPair, new_foster,
                       Decode<FosterPair>(log.redo_data));
      RETURN_IF_FAIL(target->SetFosterImpl(new_foster));
      break;
    }
    case LogType::kSystemAllocPage:
      target->PageInit(log.pid, log.allocated_page_type);
      break;
    case LogType::kSystemDestroyPage:
      // D3 (docs/design.md): destroy-redo initializes the page as a free
      // page.  The WAL record carries no free-list successor, so the chain
      // itself is rebuilt from a page-range scan when recovery finishes
      // (RecoverFrom tail); re-applying the same destroy redo only re-runs
      // this idempotent initialization.
      target->PageInit(log.pid, PageType::kFreePage);
      break;
    case LogType::kFullPageImage:
      if (log.redo_data.size() != kPageBodySize) {
        return StatusError(StatusCode::kCorrupt,
                           "full-page image has an invalid body size");
      }
      target->PageInit(log.pid, log.allocated_page_type);
      std::memcpy(&target->body, log.redo_data.data(), kPageBodySize);
      break;
    case LogType::kCompensateAllocPage:
      return StatusError(StatusCode::kCorrupt,
                         "allocation CLR requires allocator replay");
    case LogType::kCompensateDestroyPage:
      if (log.undo_data.size() != kPageBodySize) {
        return StatusError(StatusCode::kCorrupt,
                           "destroy compensation has an invalid body size");
      }
      target->PageInit(log.pid, log.allocated_page_type);
      std::memcpy(&target->body, log.undo_data.data(), kPageBodySize);
      break;
    case LogType::kSetLowFence:
    case LogType::kCompensateSetLowFence: {
      ASSIGN_OR_RETURN(IndexKey, ik, Decode<IndexKey>(log.redo_data));
      target->SetLowFenceImpl(ik);
      break;
    }
    case LogType::kSetHighFence:
    case LogType::kCompensateSetHighFence: {
      ASSIGN_OR_RETURN(IndexKey, ik, Decode<IndexKey>(log.redo_data));
      target->SetHighFenceImpl(ik);
      break;
    }
    default:
      return StatusError(StatusCode::kCorrupt, "unhandled WAL redo type");
  }
  // ARIES: a page modified during replay is dirty from the earliest applied
  // LSN. Without this the page keeps recovery_lsn == MAX (set on load), a
  // checkpoint would record it clean in the DPT, and a crash before its
  // flush would silently drop the restored effect.
  //
  // RecoveryLSN keeps the record start; PageLSN covers the record end.
  const lsn_t record_end = lsn + log.Size();
  target->SetRecLSN(lsn);
  target->SetPageLSN(record_end);
  return Status::kSuccess;
}

StatusOr<lsn_t> LogUndo(PageRef& target, lsn_t lsn, const LogRecord& log,
                        TransactionManager* tm, PagePool* pool) {
  if (log.type == LogType::kUnknown) {
    return StatusError(StatusCode::kCorrupt, "unknown log type");
  }
  // A page-type mismatch means the page was destroyed or re-issued after
  // this record was written: the operation's effects no longer exist on
  // the current incarnation, so the physical half of the undo is a no-op
  // (and would corrupt a newer incarnation). The CLR is still logged --
  // the loser chain must terminate on every future replay -- but the page
  // keeps its own stamps because none of this record's history was applied.
  const Status page_type_status = ValidateReplayPageType(*target, log.type);
  const bool apply_physical = page_type_status == Status::kSuccess;
  if (!apply_physical) {
    LOG(WARN) << "undo skips physically stale record (page incarnation "
                 "changed): lsn "
              << lsn << " page " << target->PageID() << " (" << log.type << ")";
  }
  lsn_t clr_end = 0;
  switch (log.type) {
    case LogType::kUnknown:
      return StatusError(StatusCode::kCorrupt, "unknown log type");
    case LogType::kInsertRow: {
      ASSIGN_OR_RETURN(
          lsn_t, clr7731,
          tm->CompensateInsertLog(log.txn_id, log.pid, log.slot, log.prev_lsn));
      clr_end = clr7731;
      if (apply_physical) {
        target->DeleteImpl(log.slot);
      }
      break;
    }
    case LogType::kUpdateRow: {
      ASSIGN_OR_RETURN(lsn_t, clr7947,
                       tm->CompensateUpdateLog(log.txn_id, log.pid, log.slot,
                                               log.undo_data, log.prev_lsn));
      clr_end = clr7947;
      if (apply_physical) {
        target->UpdateImpl(log.slot, log.undo_data);
      }
      break;
    }
    case LogType::kDeleteRow: {
      ASSIGN_OR_RETURN(lsn_t, clr8193,
                       tm->CompensateDeleteLog(log.txn_id, log.pid, log.slot,
                                               log.undo_data, log.prev_lsn));
      clr_end = clr8193;
      if (apply_physical) {
        const Status reinsert = target->InsertImpl(log.slot, log.undo_data);
        if (reinsert == Status::kNoSpace || reinsert == Status::kTooBigData) {
          // Availability floor, row-page twin of the leaf re-insert below:
          // committed inserts redone into the deleted row's hole can leave
          // the page without room for this loser's restore. One loser-era
          // row stays absent, loudly warned; failing the open would wedge
          // the database on WAL that is already baked in.
          LOG(WARN) << "undo row re-insert skipped (page full): lsn " << lsn
                    << " page " << log.pid << " slot " << log.slot << ": "
                    << reinsert;
        } else {
          RETURN_IF_FAIL(reinsert);
        }
      }
      break;
    }
    case LogType::kSystemDestroyPage: {
      // D3 (docs/design.md): v2 destroy records carry the destroyed page's
      // type and, when it held content, its full body image, so undoing an
      // aborted destroy restores the page exactly (a rolled-back DROP TABLE
      // keeps its rows).  A legacy v1 record (or an all-zero body) falls
      // back to type-only reinitialization; the free-list rebuild at the
      // end of recovery then treats any remaining orphan conservatively.
      //
      // WAL-before-data: log the CLR *before* mutating the page.  An
      // unlogged restore would vanish on the next crash — REDO would
      // replay the destroy and leave the page free while the catalog
      // entry (restored through a logged compensate-delete) points at
      // it.  The CLR also marks the original op compensated so a
      // crash-mid-undo never restores the image twice.  A null tm means
      // there is no logger to compensate through (single-page recovery
      // consults the log directly); the page-level fix-up still runs.
      if (tm != nullptr) {
        ASSIGN_OR_RETURN(lsn_t, clr_destroy,
                         tm->CompensateDestroyPageLog(
                             log.txn_id, log.pid, log.allocated_page_type,
                             log.undo_data, log.prev_lsn));
        clr_end = clr_destroy;
      }
      const page_id_t popped_next = target->Type() == PageType::kFreePage
                                        ? target->body.free_page.NextFreePage()
                                        : 0;
      target->PageInit(log.pid, log.allocated_page_type);
      if (!log.undo_data.empty()) {
        if (log.undo_data.size() != kPageBodySize) {
          return StatusError(StatusCode::kCorrupt,
                             "destroy undo has an invalid body size");
        }
        std::memcpy(&target->body, log.undo_data.data(), kPageBodySize);
      }
      // The restore made the page live again; drop it from the allocator's
      // free stack so the next AllocateNewPage cannot re-issue it.  A
      // distressed meta page must fail the undo, not abort the process.
      if (log.allocated_page_type != PageType::kFreePage && tm != nullptr &&
          tm->GetPageManager() != nullptr) {
        RETURN_IF_FAIL(
            tm->GetPageManager()->PopFreePageHead(log.pid, popped_next));
      }
      break;
    }
    case LogType::kInsertLeaf: {
      ASSIGN_OR_RETURN(
          lsn_t, clr9745,
          tm->CompensateInsertLog(log.txn_id, log.pid, log.key, log.prev_lsn));
      clr_end = clr9745;
      if (apply_physical) {
        target->DeleteImpl(log.key);
      }
      break;
    }
    case LogType::kInsertBranch: {
      ASSIGN_OR_RETURN(lsn_t, clr9962,
                       tm->CompensateInsertBranchLog(log.txn_id, log.pid,
                                                     log.key, log.prev_lsn));
      clr_end = clr9962;
      if (apply_physical) {
        target->DeleteBranchImpl(log.key);
      }
      break;
    }
    case LogType::kUpdateLeaf: {
      ASSIGN_OR_RETURN(lsn_t, clr10189,
                       tm->CompensateUpdateLog(log.txn_id, log.pid, log.key,
                                               log.undo_data, log.prev_lsn));
      clr_end = clr10189;
      if (apply_physical) {
        target->UpdateImpl(log.key, log.undo_data);
      }
      break;
    }
    case LogType::kUpdateBranch: {
      ASSIGN_OR_RETURN(
          lsn_t, clr10436,
          tm->CompensateUpdateBranchLog(log.txn_id, log.pid, log.key,
                                        log.undo_page, log.prev_lsn));
      clr_end = clr10436;
      if (apply_physical) {
        target->UpdateBranchImpl(log.key, log.undo_page);
      }
      break;
    }
    case LogType::kDeleteLeaf: {
      ASSIGN_OR_RETURN(lsn_t, clr10693,
                       tm->CompensateDeleteLog(log.txn_id, log.pid, log.key,
                                               log.undo_data, log.prev_lsn));
      clr_end = clr10693;
      if (apply_physical) {
        const Status reinsert = target->InsertImpl(log.key, log.undo_data);
        if (reinsert != Status::kSuccess) {
          LOG(ERROR) << "undo re-insert failed on page " << log.pid
                     << " key_size=" << log.key.size()
                     << " undo_size=" << log.undo_data.size() << " leaf_free="
                     << target->body.leaf_page.FreeSizeForTest()
                     << " leaf_rows=" << target->body.leaf_page.RowCount()
                     << " type=" << static_cast<int>(target->Type()) << ": "
                     << reinsert;
        }
        // Availability floor: the CLR is already in the WAL, so skipping the
        // physical half keeps every future replay idempotent. Failing the
        // OPEN here would wedge the database shut forever -- the deficit is
        // baked into the retained WAL, so every retry deterministically
        // fails the same way. The consequence of skipping is bounded: one
        // loser's delete stays materialized (its index entry remains
        // absent), which is the transaction's effects being rolled back
        // slightly incompletely -- vs. an unrecoverable database. The
        // runtime twin of this failure is prevented by the index-undo
        // reservations (including the fence-shrink reservations); this
        // skip covers databases whose WAL predates that accounting fix.
        if (reinsert != Status::kSuccess) {
          LOG(WARN) << "undo re-insert skipped (page full; see prior ERROR): "
                       "lsn "
                    << lsn << " page " << log.pid;
        }
      }
      break;
    }
    case LogType::kDeleteBranch: {
      ASSIGN_OR_RETURN(
          lsn_t, clr10940,
          tm->CompensateDeleteBranchLog(log.txn_id, log.pid, log.key,
                                        log.undo_page, log.prev_lsn));
      clr_end = clr10940;
      if (apply_physical) {
        // A committed destroy may have freed the child this separator
        // pointed at while the loser's delete kept it pending (an orphan
        // reclamation proved the subtree unreachable only because the
        // loser's undo had not yet restored the separator). Re-inserting
        // the separator would dangle; skip it with the same
        // stale-incarnation rationale as the type check above. A kFreePage
        // image alone cannot tell that destroy apart from ReadFrom's fresh
        // materialization of a never-issued id (past EOF or an all-zero
        // region): such a page is absent from the pool before this lookup
        // and carries no written checksum, and the separator pointed at it
        // when the delete ran -- the undo must restore it
        // (BranchPageTest.DeleteAbort).
        const bool resident_before = pool->ContainsResident(log.undo_page);
        ASSIGN_OR_RETURN(PageRef, undo_child,
                         pool->GetPageForRecovery(log.undo_page, nullptr));
        if (undo_child->Type() == PageType::kFreePage &&
            (resident_before || undo_child->ChecksumMatches())) {
          LOG(WARN) << "undo branch re-insert skipped (child page "
                    << log.undo_page << " was destroyed): lsn " << lsn
                    << " page " << log.pid;
          break;
        }
        const Status reinsert =
            target->InsertBranchImpl(log.key, log.undo_page);
        if (reinsert != Status::kSuccess) {
          LOG(ERROR) << "undo branch re-insert failed on page " << log.pid
                     << ": " << reinsert;
          LOG(WARN) << "undo branch re-insert skipped (page full; see prior "
                       "ERROR): lsn "
                    << lsn << " page " << log.pid;
        }
      }
      break;
    }
    case LogType::kLowestValue: {
      {
        ASSIGN_OR_RETURN(lsn_t, clr11200,
                         tm->CompensateSetLowestValueLog(
                             log.txn_id, log.pid, log.undo_page, log.prev_lsn));
        clr_end = clr11200;
      }
      if (apply_physical) {
        target->SetLowestValueBranchImpl(log.undo_page);
      }
      break;
    }
    case LogType::kSetRowPageNext: {
      ASSIGN_OR_RETURN(lsn_t, clr_row_next,
                       tm->CompensateSetRowPageNextLog(
                           log.txn_id, log.pid, log.undo_page, log.prev_lsn));
      clr_end = clr_row_next;
      if (apply_physical) {
        target->SetNextPageImpl(log.undo_page);
      }
      break;
    }
    case LogType::kSetRowPagePrev: {
      ASSIGN_OR_RETURN(lsn_t, clr_row_prev,
                       tm->CompensateSetRowPagePrevLog(
                           log.txn_id, log.pid, log.undo_page, log.prev_lsn));
      clr_end = clr_row_prev;
      if (apply_physical) {
        target->SetPrevPageImpl(log.undo_page);
      }
      break;
    }
    case LogType::kSetLowFence: {
      ASSIGN_OR_RETURN(IndexKey, undo_key, Decode<IndexKey>(log.undo_data));
      {
        ASSIGN_OR_RETURN(lsn_t, clr11513,
                         tm->CompensateSetLowFenceLog(log.txn_id, log.pid,
                                                      undo_key, log.prev_lsn));
        clr_end = clr11513;
      }
      if (apply_physical) {
        target->SetLowFenceImpl(undo_key);
      }
      break;
    }
    case LogType::kSetHighFence: {
      ASSIGN_OR_RETURN(IndexKey, undo_key, Decode<IndexKey>(log.undo_data));
      {
        ASSIGN_OR_RETURN(lsn_t, clr11805,
                         tm->CompensateSetHighFenceLog(log.txn_id, log.pid,
                                                       undo_key, log.prev_lsn));
        clr_end = clr11805;
      }
      if (apply_physical) {
        target->SetHighFenceImpl(undo_key);
      }
      break;
    }
    case LogType::kSetFoster: {
      ASSIGN_OR_RETURN(FosterPair, foster, Decode<FosterPair>(log.undo_data));
      {
        ASSIGN_OR_RETURN(lsn_t, clr12096,
                         tm->CompensateSetFosterLog(log.txn_id, log.pid, foster,
                                                    log.prev_lsn));
        clr_end = clr12096;
      }
      if (apply_physical) {
        const Status restored = target->SetFosterImpl(foster);
        if (restored != Status::kSuccess) {
          // Availability floor: the restored foster pair can be larger than
          // what the page (whose incarnation redid later splits) now has
          // room for. The link is advisory for navigation (a lost foster
          // degrades to a tree re-descent), so warn and continue.
          LOG(WARN) << "undo foster restore skipped (page full): lsn " << lsn
                    << " page " << log.pid << ": " << restored;
        }
      }
      break;
    }
    case LogType::kSystemAllocPage: {
      if (log.wire_version >= kAllocationPayloadWalVersion) {
        if (tm == nullptr || tm->GetPageManager() == nullptr) {
          return StatusError(StatusCode::kRuntimeError,
                             "allocation undo has no page manager");
        }
        ASSIGN_OR_RETURN(
            lsn_t, allocation_clr,
            tm->GetPageManager()->CompensateAllocation(target, lsn, log, tm));
        clr_end = allocation_clr;
      }
      break;
    }
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kFullPageImage:
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
    case LogType::kCompensateSetLowestValue:
    case LogType::kCompensateSetRowPageNext:
    case LogType::kCompensateSetRowPagePrev:
    case LogType::kCompensateDestroyPage:
    case LogType::kCompensateAllocPage:
      // Compensating log cannot undo.
      break;
    default:
      return StatusError(StatusCode::kCorrupt, "unhandled WAL undo type");
  }
  // A physically stale undo (apply_physical == false) wrote only the CLR;
  // the page's own history does not include this record, so advancing its
  // stamps here would let later redo skip records it never applied.
  if (!apply_physical) {
    return clr_end;
  }
  // RecoveryLSN keeps the undone record start; PageLSN covers both the
  // original record and the CLR that made the compensating mutation durable.
  const lsn_t record_end = lsn + log.Size();
  target->SetRecLSN(lsn);
  target->SetPageLSN(std::max(record_end, clr_end));
  // The undo just rewrote the body, so the stored checksum no longer describes
  // it. Page::IsValid() compares the stored checksum against the body, which
  // means a page recovery has repaired would look TORN to the next reader --
  // PageManager::GetPage would then hand back an empty ref and
  // BPlusTree::Open would silently bootstrap a fresh (empty) tree, losing
  // committed data. Mark it repaired instead: IsValid() stays true while
  // ChecksumMatches() keeps the page dirty so the repair reaches the file.
  target->MarkRepairedInMemory();
  return clr_end;
}

// Precondition: the page is locked by this thread.
Status PageReplay(PageRef&& target,
                  const std::vector<std::pair<lsn_t, LogRecord>>& logs,
                  const std::unordered_set<txn_id_t>& committed_txn,
                  TransactionManager* tm,
                  RecoveryManager::UndoneRecorder* undone, PagePool* pool) {
  // Redo & Undo a specific page.

  // Redo phase.
  for (const auto& lsn_log : logs) {
    const LogRecord& log = lsn_log.second;
    if (log.pid != target->PageID()) {
      return StatusError(StatusCode::kCorrupt,
                         "recovery log page id does not match target page");
    }

    const lsn_t& lsn = lsn_log.first;
    // A freshly recovered (broken) page starts at page_lsn == 0 meaning
    // *nothing* has been applied -- including a record that itself sits at
    // LSN 0. Since the kBegin removal the very first WAL record can be the
    // idempotent kSystemAllocPage at LSN 0, so treat "applied == 0" as
    // "nothing applied yet". Flushed images with page_lsn == 0 can only
    // contain such an ALLOCATE, whose redo (PageInit) is a no-op on them.
    const bool nothing_applied_yet = target->PageLSN() == 0;
    // Page stamps hold the last applied record's END LSN, so the filter
    // compares against this record's end: not-yet-applied means the page's
    // stamp (<= this record's start) is strictly below its end.
    const bool redo_target =
        nothing_applied_yet || target->PageLSN() < lsn + log.Size();
    const bool allocation_state =
        log.wire_version >= kAllocationPayloadWalVersion &&
        (log.type == LogType::kSystemAllocPage ||
         log.type == LogType::kCompensateAllocPage);
    if (allocation_state) {
      if (tm == nullptr || tm->GetPageManager() == nullptr) {
        return StatusError(StatusCode::kRuntimeError,
                           "allocation replay has no page manager");
      }
      RETURN_IF_FAIL(tm->GetPageManager()->ReplayAllocation(target, lsn, log,
                                                            redo_target));
    } else if (redo_target) {
      if (RecoveryTraceEnabled()) {
        LOG(INFO) << "redo: " << log;
      }
      const Status redo_status = LogRedo(target, lsn, log);
      if (redo_status != Status::kSuccess) {
        LOG(ERROR) << "redo failed at lsn " << lsn << " pid " << log.pid
                   << " type " << log.type << " txn " << log.txn_id << ": "
                   << redo_status.GetMessage();
      }
      RETURN_IF_FAIL(redo_status);
    }
  }

  // Undo phase.
  // A kCompensate* record in the redo window proves that the ORIGINAL
  // operation was already undone by an earlier recovery pass. Undoing it a
  // second time is only idempotent while the page state has not moved on;
  // when a LATER committed transaction legitimately wrote the same key/slot
  // again, replaying the stale undo deletes the fresh value (fuzz-found: a
  // re-inserted key vanished after the second crash). Scan backwards and
  // remember exactly which (txn, slot/key) operations were compensated.
  std::map<std::pair<txn_id_t, std::string>, int> compensated_ops;
  for (const auto& log : std::ranges::reverse_view(logs)) {
    const LogRecord& undo_log = log.second;
    const auto it = committed_txn.find(undo_log.txn_id);
    if (undo_log.pid != target->PageID()) {
      return StatusError(
          StatusCode::kCorrupt,
          "recovery undo log page id does not match target page");
    }
    // A full-page image is a system record: it is never compensated and its
    // txn_id (0) names no transaction, so it must be skipped before the
    // loser classification below could try to "undo" it.
    if (undo_log.type == LogType::kFullPageImage) {
      continue;
    }
    if (undone != nullptr && undone->Contains(log.first)) {
      continue;
    }
    const std::optional<LogType> closes = CompensatedOpType(undo_log.type);
    const LogType match_type = closes.value_or(undo_log.type);
    const std::string op_id = undo_log.HasSlot()
                                  ? "s" + std::to_string(undo_log.slot)
                                  : "k" + undo_log.key;
    const std::pair<txn_id_t, std::string> op_key{
        undo_log.txn_id, std::to_string(static_cast<int>(match_type)) + op_id};
    if (closes.has_value()) {
      // A compensating record closes the most recent still-open matching
      // operation of this transaction on this page (it can only refer to a
      // LOWER LSN, which the backward scan has not visited yet).
      ++compensated_ops[op_key];
      continue;
    }
    if (it == committed_txn.end()) {
      auto done_it = compensated_ops.find(op_key);
      if (done_it != compensated_ops.end() && done_it->second > 0) {
        // Already undone by a previous recovery pass (matching CLR found at
        // a newer LSN); skipping is also what makes redo/undo safe to re-run.
        --done_it->second;
        continue;
      }
      if (RecoveryTraceEnabled()) {
        LOG(INFO) << "undo: " << undo_log;
      }
      RETURN_IF_FAIL(
          LogUndo(target, log.first, undo_log, tm, pool).GetStatus());
      if (undone != nullptr) {
        undone->Record(log.first);
      }
    }
  }

  // Release the page latch.
  if (RecoveryTraceEnabled()) {
    LOG(INFO) << "SPR " << target->PageID() << " finished";
  }
  // Redo rewrote the body above; mark it repaired for the same reason as in
  // LogUndo: a page this pass repaired must not read as torn afterwards.
  target->MarkRepairedInMemory();
  target.PageUnlock();
  return Status::kSuccess;
}

size_t RecoveryWorkerCount(size_t jobs) {
  if (jobs <= 1) {
    return jobs;
  }
  constexpr size_t kMaxRecoveryWorkers = 16;
  size_t workers = std::thread::hardware_concurrency();
  if (workers == 0) {
    workers = 1;
  }
  if (const char* env = std::getenv("TINYLAMB_RECOVERY_WORKERS");
      env != nullptr && env[0] != '\0') {
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(env, &end, 10);
    // Accept only a fully numeric value; "12abc" or garbage keeps the
    // hardware default instead of silently adopting a partial parse.
    if (end != env && *end == '\0' && parsed > 0) {
      workers = parsed;
    }
  }
  return std::min({jobs, workers, kMaxRecoveryWorkers});
}

Status ReplayPagesInParallel(
    PagePool* pool,
    std::vector<std::pair<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>>*
        jobs,
    const std::unordered_set<txn_id_t>& committed_txn, TransactionManager* tm,
    RecoveryManager::UndoneRecorder* undone) {
  if (jobs->empty()) {
    return Status::kSuccess;
  }
  const size_t workers = RecoveryWorkerCount(jobs->size());
  auto replay_one = [&](size_t index) -> Status {
    auto& [page_id, logs] = (*jobs)[index];
    // Lock and unlock on this worker thread. Moving a PageRef (and its
    // unique_lock) across threads is undefined for std::shared_mutex.
    ASSIGN_OR_RETURN(PageRef, page, pool->GetPage(page_id, nullptr));
    return PageReplay(std::move(page), logs, committed_txn, tm, undone, pool);
  };
  if (workers <= 1) {
    for (size_t i = 0; i < jobs->size(); ++i) {
      Status status = Status::kSuccess;
      try {
        status = replay_one(i);
      } catch (const std::exception& ex) {
        status = StatusError(StatusCode::kRuntimeError, ex.what());
      } catch (...) {
        status = StatusError(StatusCode::kRuntimeError,
                             "unknown recovery worker failure");
      }
      RETURN_IF_FAIL(status);
    }
    return Status::kSuccess;
  }

  std::atomic<size_t> next{0};
  std::mutex error_mutex;
  Status first_error = Status::kSuccess;
  auto worker = [&]() {
    for (;;) {
      const size_t index = next.fetch_add(1, std::memory_order_relaxed);
      if (index >= jobs->size()) {
        return;
      }
      Status status = Status::kSuccess;
      try {
        status = replay_one(index);
      } catch (const std::exception& ex) {
        status = StatusError(StatusCode::kRuntimeError, ex.what());
      } catch (...) {
        status = StatusError(StatusCode::kRuntimeError,
                             "unknown recovery worker failure");
      }
      if (status != Status::kSuccess) {
        std::scoped_lock lock(error_mutex);
        if (first_error == Status::kSuccess) {
          first_error = status;
        }
        next.store(jobs->size(), std::memory_order_relaxed);
        return;
      }
    }
  };

  // jthread: a spawn failure (pthread_create EAGAIN) must not strand the
  // already-started workers as joinable threads in a throwing vector dtor
  // (std::terminate); jthread's destructor joins them instead.
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  for (size_t i = 0; i < workers; ++i) {
    try {
      threads.emplace_back(worker);
    } catch (const std::exception& ex) {
      std::scoped_lock lock(error_mutex);
      first_error = StatusError(StatusCode::kRuntimeError, ex.what());
      next.store(jobs->size(), std::memory_order_relaxed);
      break;
    } catch (...) {
      std::scoped_lock lock(error_mutex);
      first_error = StatusError(StatusCode::kRuntimeError,
                                "failed to start recovery worker");
      next.store(jobs->size(), std::memory_order_relaxed);
      break;
    }
  }
  for (std::jthread& thread : threads) {
    thread.join();
  }
  return first_error;
}

// The (txn, op, page, slot/key) identity a loser-undo skip decision is keyed
// by. Shared by PageReplay's per-page compensated_ops and the global
// UndoLoserChains evidence map, so both passes match CLRs to the operations
// they close in exactly the same way.
std::string UndoOpId(const LogRecord& log) {
  return log.HasSlot() ? "s" + std::to_string(log.slot) : "k" + log.key;
}

std::tuple<txn_id_t, int, page_id_t, std::string> CompensatedOpKey(
    const LogRecord& clr, LogType closed_type) {
  return {clr.txn_id, static_cast<int>(closed_type), clr.pid, UndoOpId(clr)};
}

// A loser kInsertBranch record, remembered by the analysis pass for the
// post-undo separator sweep (the undo's physical half may have been skipped
// by the page-incarnation type check while the child's allocation undo still
// freed the child -- leaving a live branch routing into a free page).
struct LoserBranchInsert {
  txn_id_t txn;
  lsn_t lsn;
  page_id_t parent;
  page_id_t child;
};

// A loser SetFoster record, remembered by the analysis pass for the
// post-undo foster sweep (the same shape as LoserBranchInsert, but the
// routing link lives on the splitting page itself -- a leaf root's pending
// height growth -- instead of on a branch parent).
struct LoserFosterSet {
  txn_id_t txn;
  lsn_t lsn;
  page_id_t parent;
  page_id_t child;
};

// True when the anchor record's REDO restores a full page body (and can
// therefore resurrect state earlier passes already compensated).
bool AnchorRestoresBody(const LogRecord& anchor) {
  return anchor.type == LogType::kFullPageImage ||
         anchor.type == LogType::kCompensateDestroyPage ||
         (anchor.type == LogType::kCompensateAllocPage &&
          anchor.undo_data.size() == kPageBodySize);
}

bool IsReinitAnchor(LogType type) {
  return type == LogType::kSystemAllocPage ||
         type == LogType::kCompensateAllocPage ||
         type == LogType::kFullPageImage ||
         type == LogType::kSystemDestroyPage ||
         type == LogType::kCompensateDestroyPage;
}

// Test-only deterministic mid-undo crash point: TINYLAMB_TEST_UNDO_STOP_AFTER
// = N makes UndoLoserChains stop after N undo applications (the applied CLRs
// are already in the WAL), so recovery_extra_test can build partial-pass undo
// histories -- the multi-restart shape that used to resurrect compensated
// state through body-restoring anchors. Read per call (not latched) so later
// reopens in the same process run full walks.
int TestUndoStopAfter() {
  const char* env = std::getenv("TINYLAMB_TEST_UNDO_STOP_AFTER");
  if (env == nullptr || env[0] == '\0') {
    return 0;
  }
  char* end = nullptr;
  const long parsed = std::strtol(env, &end, 10);
  if (end == env || *end != '\0' || parsed <= 0) {
    return 0;
  }
  return static_cast<int>(parsed);
}

}  // namespace

RecoveryManager::RecoveryManager(std::string_view log_path, PagePool* pp,
                                 size_t segment_size)
    : log_name_(log_path), segment_size_(segment_size), pool_(pp) {}

RecoveryManager::ReadFd::~ReadFd() {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

RecoveryManager::~RecoveryManager() {
  std::scoped_lock lock(read_fd_mutex_);
  segment_fds_.clear();
}

std::shared_ptr<RecoveryManager::ReadFd> RecoveryManager::SegmentFdFor(
    lsn_t lsn) const {
  const size_t index = Logger::SegmentIndex(lsn, segment_size_);
  std::scoped_lock lock(read_fd_mutex_);
  const auto found = segment_fds_.find(index);
  if (found != segment_fds_.end()) {
    return found->second;
  }
  const std::filesystem::path path = Logger::SegmentPath(log_name_, index);
  std::error_code ec;
  const bool present = std::filesystem::exists(path, ec);
  if (ec || !present) {
    return {};
  }
  const int raw_fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (raw_fd < 0) {
    return {};
  }
  std::shared_ptr<ReadFd> fd;
  try {
    fd = std::make_shared<ReadFd>(raw_fd);
  } catch (...) {
    ::close(raw_fd);
    return {};
  }
  if (segment_fds_.size() >= kMaxCachedReadFds) {
    auto oldest = segment_fds_.begin();
    for (auto it = segment_fds_.begin(); it != segment_fds_.end(); ++it) {
      if (it->first < oldest->first) {
        oldest = it;
      }
    }
    segment_fds_.erase(oldest);
  }
  segment_fds_.emplace(index, fd);
  return fd;
}

lsn_t RecoveryManager::LogStart() const {
  const auto status = LogStartStatus();
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::LogStartStatus() const {
  bool any = false;
  auto scanned = Logger::FindLowestSegment(log_name_, &any);
  if (!scanned.HasValue()) {
    return scanned.GetStatus();
  }
  if (!any) {
    return lsn_t{0};
  }
  return static_cast<lsn_t>(scanned.Value()) *
         static_cast<lsn_t>(segment_size_);
}

lsn_t RecoveryManager::LogEnd() const {
  const auto status = LogEndStatus();
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::LogEndStatus() const {
  bool any = false;
  auto scanned = Logger::FindHighestSegment(log_name_, &any);
  if (!scanned.HasValue()) {
    return scanned.GetStatus();
  }
  if (!any) {
    return lsn_t{0};
  }
  const uint64_t highest = scanned.Value();
  const lsn_t base =
      static_cast<lsn_t>(highest) * static_cast<lsn_t>(segment_size_);
  std::error_code ec;
  const auto size =
      std::filesystem::file_size(Logger::SegmentPath(log_name_, highest), ec);
  if (ec) {
    return StatusError(StatusCode::kIOError,
                       "failed to stat WAL segment: " + ec.message());
  }
  if (size > std::numeric_limits<lsn_t>::max() - base) {
    return StatusError(StatusCode::kIOError, "WAL end LSN overflows");
  }
  return base + static_cast<lsn_t>(size);
}

RecoveryManager::ReadAtResult RecoveryManager::ReadAtDetailed(
    lsn_t lsn, char* dst, size_t len) const {
  if (len == 0) {
    return {.bytes = 0, .status = ReadAtStatus::kComplete};
  }
  // Volatile tail first: a runtime undo walk reads its own records back
  // through the live logger's ring buffer while they are still buffered
  // (the segment files cannot serve them yet). Restart-time scans see an
  // empty ring window and take the file path below unchanged.
  if (logger_ != nullptr && logger_->TryCopyBuffered(lsn, dst, len)) {
    return {.bytes = len, .status = ReadAtStatus::kComplete};
  }
  size_t done = 0;
  while (done < len) {
    const lsn_t absolute = lsn + static_cast<lsn_t>(done);
    const auto fd = SegmentFdFor(absolute);
    if (!fd) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    const auto offset =
        static_cast<size_t>(absolute % static_cast<lsn_t>(segment_size_));
    const size_t want = std::min(len - done, segment_size_ - offset);
    struct stat file_stat{};
    if (::fstat(fd->fd, &file_stat) != 0) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    ssize_t nread = 0;
    do {
      nread = ::pread(fd->fd, dst + done, want, static_cast<off_t>(offset));
    } while (nread < 0 && errno == EINTR);
    if (nread < 0) {
      return {.bytes = done, .status = ReadAtStatus::kUnavailable};
    }
    const auto bytes_read = static_cast<size_t>(std::max<ssize_t>(nread, 0));
    const bool reached_file_end =
        std::cmp_greater_equal(offset + bytes_read, file_stat.st_size);
    if (nread == 0) {
      if (!reached_file_end) {
        return {.bytes = done, .status = ReadAtStatus::kShort};
      }
      return {
          .bytes = done,
          .status = done == 0 ? ReadAtStatus::kEof : ReadAtStatus::kTruncated};
    }
    done += bytes_read;
    if (bytes_read < want) {
      if (!reached_file_end) {
        return {.bytes = done, .status = ReadAtStatus::kShort};
      }
      if (done >= len) {
        return {.bytes = done, .status = ReadAtStatus::kTruncated};
      }
      const lsn_t next_lsn = lsn + static_cast<lsn_t>(done);
      const auto next = SegmentFdFor(next_lsn);
      const size_t current_index =
          Logger::SegmentIndex(absolute, segment_size_);
      const size_t next_index = Logger::SegmentIndex(next_lsn, segment_size_);
      if (next && next_index != current_index) {
        if (offset + bytes_read == segment_size_ &&
            offset + want == segment_size_) {
          continue;
        }
        return {.bytes = done, .status = ReadAtStatus::kUnavailable};
      }
      return {.bytes = done, .status = ReadAtStatus::kTruncated};
    }
  }
  return {.bytes = done, .status = ReadAtStatus::kComplete};
}

std::optional<lsn_t> RecoveryManager::PreRollPaddingEnd(lsn_t lsn) const {
  if (segment_size_ == 0) {
    return std::nullopt;
  }
  const size_t segment = Logger::SegmentIndex(lsn, segment_size_);
  const lsn_t boundary =
      (static_cast<lsn_t>(segment) + 1) * static_cast<lsn_t>(segment_size_);
  if (lsn >= boundary) {
    return std::nullopt;
  }
  std::array<char, 512> buffer{};
  lsn_t at = lsn;
  while (at < boundary) {
    const size_t want =
        static_cast<size_t>(std::min<lsn_t>(buffer.size(), boundary - at));
    const ReadAtResult read = ReadAtDetailed(at, buffer.data(), want);
    if (read.status != ReadAtStatus::kComplete || read.bytes != want) {
      return std::nullopt;
    }
    for (size_t i = 0; i < read.bytes; ++i) {
      if (buffer[i] != 0) {
        return std::nullopt;
      }
    }
    at += read.bytes;
  }
  if (SegmentFdFor(boundary) == nullptr) {
    return std::nullopt;
  }
  return boundary;
}

size_t RecoveryManager::ReadAt(lsn_t lsn, char* dst, size_t len) const {
  return ReadAtDetailed(lsn, dst, len).bytes;
}

Status RecoveryManager::SinglePageRecovery(
    PageRef&& page, TransactionManager* tm,
    UndoneRecorder* undone) {  // NOLINT(readability-make-member-function-const)
  ASSIGN_OR_RETURN(lsn_t, log_start, LogStartStatus());
  ASSIGN_OR_RETURN(lsn_t, log_end, LogEndStatus());
  return SinglePageRecovery(std::move(page), tm, undone, log_start, log_end,
                            nullptr);
}

Status RecoveryManager::
    SinglePageRecovery(  // NOLINT(readability-make-member-function-const)
        PageRef&& page, TransactionManager* tm, UndoneRecorder* undone,
        lsn_t scan_start, lsn_t scan_end,
        const std::unordered_set<txn_id_t>* known_committed) {
  const bool page_was_invalid = !page->IsValid();
  ASSIGN_OR_RETURN(lsn_t, log_end, LogEndStatus());
  const lsn_t filesize = std::min(log_end, scan_end);
  std::vector<std::pair<lsn_t, LogRecord>> page_logs;
  std::unordered_set<txn_id_t> committed_txn;
  if (known_committed != nullptr) {
    // A recycled WAL window cannot show which transactions committed below
    // scan_start; the caller's recovery analysis is authoritative there.
    committed_txn.insert(known_committed->begin(), known_committed->end());
  }
  {
    lsn_t offset = scan_start;
    LogRecord log;
    while (offset < filesize) {
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status == LogReadStatus::kUnavailable) {
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (IsPageManipulation(log.type) && log.pid == page->PageID()) {
        page_logs.emplace_back(offset, log);
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  if (page_was_invalid && page_logs.empty()) {
    return StatusError(
        StatusCode::kCorrupt,
        "no WAL records for invalid page " + std::to_string(page->PageID()));
  }
  return PageReplay(std::move(page), page_logs, committed_txn, tm, undone,
                    pool_);
}

Status RecoveryManager::RecoverBrokenPages(
    const std::vector<page_id_t>& broken_ids, TransactionManager* tm,
    UndoneRecorder* undone, lsn_t scan_start, lsn_t scan_end,
    const std::unordered_set<txn_id_t>* known_committed,
    std::unordered_map<page_id_t, lsn_t>* body_restore_anchors) {
  if (broken_ids.empty()) {
    return Status::kSuccess;
  }
  const std::unordered_set<page_id_t> wanted(broken_ids.begin(),
                                             broken_ids.end());
  std::unordered_set<txn_id_t> committed_txn;
  if (known_committed != nullptr) {
    committed_txn.insert(known_committed->begin(), known_committed->end());
  }
  // One pass over the window collects every broken page's records, then each
  // page replays ONCE. Records are kept whole until the scan finishes
  // because a recycled page id's history contains OLDER INCARNATIONS: a
  // page torn in its current (leaf) incarnation still logs [row ops v1,
  // ALLOC v2, row ops v2, DESTROY, ALLOC v3, leaf ops v3] under the same
  // pid, and replaying the v1 records onto the torn v3 image fails the
  // replay type check before the v3 re-init record ever arrives. The replay
  // therefore starts at each page's LAST re-initialization record
  // (kSystemAllocPage / kCompensateAllocPage / kFullPageImage /
  // kSystemDestroyPage / kCompensateDestroyPage) -- everything before it
  // belongs to superseded incarnations and the re-init restores (or
  // empties) the full body anyway. A page with no re-init record in the
  // window was born before it: its records replay from the window start
  // exactly as the per-page SinglePageRecovery always did.
  // Memory: bounded by the torn pages' share of the window, not the whole
  // window (the redo phase below keeps the whole-DPT profile batch-shaped).
  std::unordered_map<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>
      window;
  {
    // scan_end (the validated log end) never exceeds the readable bytes.
    lsn_t offset = scan_start;
    const lsn_t filesize = scan_end;
    LogRecord log;
    while (offset < filesize) {
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status == LogReadStatus::kUnavailable) {
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      } else if (IsPageManipulation(log.type) && wanted.contains(log.pid)) {
        window[log.pid].emplace_back(offset, log);
      }
      offset += log.Size();
    }
  }
  for (auto& [pid, logs] : window) {
    if (logs.empty()) {
      continue;
    }
    size_t incarnation_birth = 0;
    for (size_t i = logs.size(); i > 0; --i) {
      if (IsReinitAnchor(logs[i - 1].second.type)) {
        incarnation_birth = i - 1;
        break;
      }
    }
    if (incarnation_birth > 0) {
      logs.erase(logs.begin(),
                 logs.begin() + static_cast<std::ptrdiff_t>(incarnation_birth));
    }
    // An anchor whose redo RESTORES A FULL BODY resurrects whatever the body
    // held -- including loser state earlier passes already compensated. The
    // loser walk must then treat undo evidence older than this anchor as
    // void (it re-attempts the undo; LogUndo's type check keeps a recycled
    // incarnation safe). kSystemAllocPage/kSystemDestroyPage anchors only
    // PageInit an empty body, so nothing is resurrected there.
    if (body_restore_anchors != nullptr && !logs.empty() &&
        IsReinitAnchor(logs.front().second.type) &&
        AnchorRestoresBody(logs.front().second)) {
      (*body_restore_anchors)[pid] = logs.front().first;
    }
    ASSIGN_OR_RETURN(PageRef, page, pool_->GetPageForRecovery(pid, nullptr));
    // A torn image's stored page_lsn is garbage: anchor the replay at zero.
    page->page_lsn = 0;
    page->page_id = pid;
    LOG(INFO) << "Page " << pid << " is broken, start SPR (" << logs.size()
              << " record(s)).";
    RETURN_IF_FAIL(
        PageReplay(std::move(page), logs, committed_txn, tm, undone, pool_));
  }
  std::unordered_set<page_id_t> rebuilt;
  for (const auto& [pid, logs] : window) {
    rebuilt.insert(pid);
  }
  for (const page_id_t broken_id : broken_ids) {
    if (rebuilt.contains(broken_id)) {
      continue;
    }
    // Same verdict as the per-page SPR: a broken image the retained window
    // cannot explain is a fail-closed corruption, not a rebuildable page.
    ASSIGN_OR_RETURN(PageRef, page,
                     pool_->GetPageForRecovery(broken_id, nullptr));
    if (!page->IsValid()) {
      return StatusError(
          StatusCode::kCorrupt,
          "no WAL records for invalid page " + std::to_string(broken_id));
    }
  }
  return Status::kSuccess;
}

Status RecoveryManager::RecoverFrom(lsn_t checkpoint_lsn,
                                    TransactionManager* tm) {
  const auto recovery_started = std::chrono::steady_clock::now();
  ASSIGN_OR_RETURN(lsn_t, on_disk, LogEndStatus());
  if (on_disk == 0) {
    if (checkpoint_lsn != 0) {
      return StatusError(StatusCode::kCorrupt,
                         "checkpoint exists but WAL is empty: " + log_name_);
    }
    LOG(INFO) << "Log file absent or empty, skipping recovery: " << log_name_;
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(lsn_t, log_start, LogStartStatus());

  // Recycling guard: a recycled prefix without a valid master record means
  // the checkpoint that anchored the retained WAL is gone. Replaying from
  // the retained start anyway would silently drop every transaction between
  // the last full replay point and the anchor -- fail closed instead.
  if (checkpoint_lsn == 0 && log_start > 0) {
    LOG(FATAL) << "WAL prefix was recycled (retained from " << log_start
               << ") but no usable master record exists; refusing to "
               << "replay a partial history of " << log_name_;
  }
  if (checkpoint_lsn < log_start) {
    LOG(FATAL) << "Master record checkpoint LSN " << checkpoint_lsn
               << " points into recycled WAL (retained from " << log_start
               << "); the checkpoint anchoring the retained log is missing.";
  }

  // Torn tail policy (single authority, section 1.9): the first unparseable
  // offset ends the log; everything past it is a half-written append from the
  // crash and is truncated here so all later passes share one log end.
  UndoneRecorder undone;
  const lsn_t tail_scan_from = std::max(checkpoint_lsn, log_start);
  RecoveryProgressLogger tail_progress("torn-tail scan", tail_scan_from,
                                       on_disk);
  ASSIGN_OR_RETURN(lsn_t, valid_end,
                   ValidLogEndStatus(tail_scan_from, [&](lsn_t at) {
                     tail_progress.Tick(at);
                   }));
  if (valid_end < on_disk) {
    const TailDamage damage = ClassifyTailDamage(valid_end, on_disk);
    if (!TornTailTruncationAllowed() && damage == TailDamage::kSuspect) {
      // Damage that may hide committed history (mid-file garbage, a missing
      // middle segment): truncating would silently drop everything after the
      // corruption point, so without the operator's --force, fail closed.
      LOG(FATAL) << "WAL corruption at offset " << valid_end << " of "
                 << log_name_ << " (" << (on_disk - valid_end)
                 << " unparsable bytes, damage class "
                 << static_cast<int>(damage)
                 << "). Restart with --force to truncate the damaged tail "
                 << "and recover its intact prefix.";
    }
    // Provable tail damage (a record cut by the crash, a zero-filled
    // never-written region): the truncated bytes were never acknowledged
    // durable, so recovery proceeds automatically -- an unattended restart
    // must not wedge on the last append of the previous life.
    LOG(WARN) << "Truncating torn log tail: " << on_disk << " -> " << valid_end
              << " (damage class " << static_cast<int>(damage) << ")";
    // TruncateTo performs the file surgery (tail resize + stale segment
    // removal) and resyncs the Logger's three LSNs, which its constructor
    // latched to the pre-truncation file size.  O_APPEND writes would
    // otherwise land at the new EOF while AddLog kept reporting offsets past
    // it, desynchronizing every page stamp and loser-chain walk from the
    // files.  Resync now.
    RETURN_IF_FAIL(tm->logger_->TruncateTo(valid_end));
  }
  const lsn_t filesize = std::min(on_disk, valid_end);

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
  page_id_t max_seen_pid = 0;
  std::unordered_map<txn_id_t, page_id_t> max_pid_by_txn;
  std::unordered_set<txn_id_t> v6_allocation_txns;
  std::unordered_set<txn_id_t> compensated_allocation_txns;
  page_id_t checkpoint_max_pid = 0;
  // Highest txn id the WAL ever used.  The TransactionManager counter is
  // not WAL'd, so without re-deriving it a restart would re-issue ids that
  // old kCommit records already vouch for, hiding new losers from undo.
  txn_id_t max_seen_txn = 0;
  // WAL evidence of undo work done by EARLIER recovery passes: every
  // kCompensate* record in the window proves the operation it closes was
  // physically undone before this restart. UndoLoserChains consults it
  // because a loser chain's CLR threading only redirects the walk past
  // operations compensated by passes whose CLRs sit ON the chain -- a
  // re-walk after a page has since been re-issued to a COMMITTED allocation
  // must not re-apply the old undo's physical mutation (session-fuzzer:
  // tbl1's page vanished when a second restart re-freed a recycled page).
  // Key -> {LSN of the newest closing CLR, its end LSN}.
  std::map<std::tuple<txn_id_t, int, page_id_t, std::string>,
           std::pair<lsn_t, lsn_t>>
      loser_compensation_evidence;
  // Loser branch separator inserts for the post-undo sweep: an undo whose
  // physical half is skipped by the page-incarnation type check can leave a
  // live branch routing into a page the same recovery frees (soak finding:
  // "descent routed to a non-branch page" after a torn restart).
  std::vector<LoserBranchInsert> loser_branch_inserts;
  std::vector<LoserFosterSet> loser_foster_sets;
  // Routing links set BEFORE the checkpoint live in checkpoint FPW images but
  // their records sit below the analysis window; a pre-scan of the retained
  // history collects them (plus commits, to keep the loser filter honest).
  // Over-collecting is harmless: the sweep itself only removes a link whose
  // child page is still free when recovery ends -- a committed transaction's
  // child never qualifies.
  const lsn_t prescan_end = std::max(checkpoint_lsn, log_start);
  if (log_start < prescan_end) {
    lsn_t offset = log_start;
    LogRecord log;
    while (offset < prescan_end) {
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status != LogReadStatus::kComplete) {
        // Segment-tail pre-roll padding parses as garbage (the analysis pass
        // skips it via PreRollPaddingEnd). This prescan must do the same: it
        // feeds committed_txn, and a silent stop at the FIRST padding region
        // drops every commit between there and the anchor while the redo pass
        // still replays from the DPT's recLSN floor below -- the per-page
        // undo then classifies those committed transactions as losers and
        // physically deletes their rows and frees their pages (a checkpointed
        // heavy load lost millions of rows this way on the next restart).
        // Actual damage (non-padding garbage, a missing segment) still stops
        // the scan -- the main analysis re-validates anything it needs.
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        break;  // the main analysis re-validates anything it needs
      }
      if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      } else if (log.type == LogType::kInsertBranch && log.redo_page != 0) {
        loser_branch_inserts.push_back(
            {log.txn_id, offset, log.pid, log.redo_page});
      } else if (log.type == LogType::kSetFoster) {
        if (auto decoded = Decode<FosterPair>(log.redo_data);
            decoded.HasValue() && decoded.Value().child_pid != 0) {
          loser_foster_sets.push_back(
              {log.txn_id, offset, log.pid, decoded.Value().child_pid});
        }
      }
      offset += log.Size();
    }
  }
  {
    const lsn_t analysis_from = std::max(checkpoint_lsn, log_start);
    RecoveryProgressLogger analysis_progress("analysis", analysis_from,
                                             filesize);
    lsn_t offset = analysis_from;
    LogRecord log;
    while (offset < filesize) {
      analysis_progress.Tick(offset);
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status == LogReadStatus::kUnavailable) {
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (RecoveryTraceEnabled()) {
        LOG(TRACE) << "analyzing: " << offset << ": " << log
                   << "  to: " << offset + log.Size();
      }
      if (log.type != LogType::kEndCheckpoint &&
          log.type != LogType::kBeginCheckpoint) {
        txn_last_lsn[log.txn_id] = offset;
        max_seen_txn = std::max(max_seen_txn, log.txn_id);
      }
      if (IsPageManipulation(log.type)) {
        // Collect the oldest LSN to dirty_page_table.
        UpdateOldestLSN(log.pid, offset);
        max_seen_pid = std::max(max_seen_pid, log.pid);
        max_pid_by_txn[log.txn_id] =
            std::max(max_pid_by_txn[log.txn_id], log.pid);
        if (log.type == LogType::kSystemAllocPage &&
            log.wire_version >= kAllocationPayloadWalVersion) {
          v6_allocation_txns.insert(log.txn_id);
        } else if (log.type == LogType::kCompensateAllocPage) {
          compensated_allocation_txns.insert(log.txn_id);
        }
        if (const auto closed = CompensatedOpType(log.type);
            closed.has_value()) {
          auto& evidence =
              loser_compensation_evidence[CompensatedOpKey(log, *closed)];
          if (evidence.first < offset) {
            evidence = {offset, offset + log.Size()};
          }
        }
        if (log.type == LogType::kInsertBranch && log.redo_page != 0) {
          loser_branch_inserts.push_back(
              {log.txn_id, offset, log.pid, log.redo_page});
        } else if (log.type == LogType::kSetFoster) {
          if (auto decoded = Decode<FosterPair>(log.redo_data);
              decoded.HasValue() && decoded.Value().child_pid != 0) {
            loser_foster_sets.push_back(
                {log.txn_id, offset, log.pid, decoded.Value().child_pid});
          }
        }
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
          checkpoint_max_pid = std::max(checkpoint_max_pid, dpt.first);
        }
        for (const auto& at : log.active_transaction_table) {
          max_seen_txn = std::max(max_seen_txn, at.txn_id);
          if (at.status == TransactionStatus::kCommitted) {
            committed_txn.insert(at.txn_id);
          } else if (at.status == TransactionStatus::kRunning) {
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

  // Resume the id counter above every id the WAL still remembers; see
  // TransactionManager::SeedNextTransactionId.
  if (tm != nullptr && max_seen_txn != std::numeric_limits<txn_id_t>::max()) {
    tm->SeedNextTransactionId(max_seen_txn + 1);
  }

  // Loser chain heads: newest LSN of every transaction without a commit
  // record, from both the scanned region and the checkpoint's active
  // transaction table (covers losers whose writes precede checkpoint_lsn).
  // Txn 0 is the system transaction (allocations, destroys, full-page
  // images): it never commits, but its records are recovery machinery, not
  // an uncommitted transaction -- walking them as a loser chain would let
  // the head record's undo (e.g. a system allocation) free a page a
  // committed transaction may own.
  std::vector<lsn_t> loser_heads;
  for (const auto& [txn, lsn] : txn_last_lsn) {
    if (txn == 0) {
      continue;
    }
    if (!committed_txn.contains(txn)) {
      loser_heads.push_back(lsn);
    }
  }
  for (const auto& [txn, lsn] : att_running_lsn) {
    if (txn == 0) {
      continue;
    }
    if (!committed_txn.contains(txn)) {
      const auto it = txn_last_lsn.find(txn);
      if (it != txn_last_lsn.end() && it->second > lsn) {
        continue;
      }
      if (it == txn_last_lsn.end() && lsn == 0) {
        LogRecord first;
        if (!ReadLog(0, &first) || first.txn_id != txn) {
          continue;
        }
      }
      loser_heads.push_back(lsn);
    }
  }
  std::ranges::sort(loser_heads, std::greater<>());

  // Undo-chain walks and broken-page rebuilds read backwards through the
  // WAL; never below the retained start. With the full history on disk
  // (log_start == 0) those scans keep reading from LSN 0 exactly as before.
  const lsn_t scan_start =
      log_start == 0 ? 0 : std::min(log_start, redo_start_point);
  const std::unordered_set<txn_id_t>* known_committed =
      log_start == 0 ? nullptr : &committed_txn;

  // Classify every dirty page first, without keeping any of them latched
  // (streaming redo below fetches its own targets per batch). Rebuilding a
  // torn page re-enters the allocator
  // (PageManager::ReplayAllocation
  // -> GetMetaPage), and page 0 carries a kFullPageImage, so it is normally a
  // DPT member: holding its PageRef while running SPR would make the rebuild
  // re-lock a std::shared_mutex this thread already owns, and libstdc++ reports
  // that as std::system_error(resource_deadlock_would_occur).
  std::vector<page_id_t> broken_pages;
  broken_pages.reserve(dirty_page_table.size());
  for (const auto& it : dirty_page_table) {
    // A broken on-disk image must reach the SPR check below verbatim, so this
    // loop bypasses PagePool checksum enforcement.
    ASSIGN_OR_RETURN(PageRef, page,
                     pool_->GetPageForRecovery(it.first, nullptr));
    if (!page->IsValid()) {
      broken_pages.push_back(it.first);
    }
  }

  // Rebuild every torn image before any other dirty page stays latched. One
  // shared WAL pass serves all of them: the per-page SinglePageRecovery this
  // replaced re-read the whole window once PER broken page, and a crash
  // under sustained load tears every hot page (a 400 MiB window x ~hundreds
  // of pages made the restart take tens of minutes at ~2.3 s per page).
  std::unordered_map<page_id_t, lsn_t> body_restore_anchors;
  RETURN_IF_FAIL(RecoverBrokenPages(broken_pages, tm, &undone, scan_start,
                                    valid_end, known_committed,
                                    &body_restore_anchors));

  // Streaming redo. The window between the last successful checkpoint and
  // the WAL end can be gigabytes when checkpointing kept failing under a
  // long-lived process; holding every page-manipulation record in memory made
  // the restart's memory profile grow with the WINDOW instead of with the
  // database. The window is therefore replayed in bounded batches: the
  // analysis pass above already produced the complete committed set, so a
  // batch boundary can never reclassify a transaction, and PageReplay is
  // idempotent per page (page_lsn guards skip records a previous batch
  // applied), so a page spanning batches is replayed once per batch it
  // appears in with no double-apply. Per batch, records group by page in
  // ascending LSN exactly as the whole-window collection did.
  //
  // The dirty pages are no longer pre-latched for the whole redo phase (the
  // old whole-window collection pinned every DPT page); each batch's workers
  // fetch and latch their own targets. Recovery runs before the database
  // serves transactions, so the transient unlatched spans are invisible.
  std::unordered_set<page_id_t> redo_pages;
  redo_pages.reserve(dirty_page_table.size());
  for (const auto& [pid, rec_lsn] : dirty_page_table) {
    redo_pages.insert(pid);
  }
  // ARIES redo starts at each page's RecoveryLSN, not at the window start.
  // The page_lsn guard usually covers the difference, but a DESTROYED page
  // whose free image reached the disk carries page_lsn 0: without the
  // per-page floor, every record ever logged for that pid above the window
  // start replays onto the free image and fails the type check (an old
  // INSERT LEAF on what is now a free page). Records below the page's
  // recLSN can never be part of its current incarnation's history.
  const auto rec_lsn_of = [&dirty_page_table](page_id_t pid) -> lsn_t {
    const auto it = dirty_page_table.find(pid);
    return it == dirty_page_table.end() ? 0 : it->second;
  };
  {
    // Bound one batch at ~64 MiB of records or 16k records (an FPW record
    // alone is 32 KiB, so neither bound alone suffices).
    constexpr size_t kRedoWindowRecords = size_t{16} * 1024U;
    constexpr size_t kRedoWindowBytes = size_t{64} << 20;
    const lsn_t collect_from = std::max(redo_start_point, log_start);
    RecoveryProgressLogger collect_progress("redo replay", collect_from,
                                            filesize);
    lsn_t offset = collect_from;
    std::unordered_map<page_id_t, size_t> window_index;
    std::vector<std::pair<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>>
        window;
    size_t window_records = 0;
    size_t window_bytes = 0;
    auto flush_window = [&]() -> Status {
      const Status replayed =
          ReplayPagesInParallel(pool_, &window, committed_txn, tm, &undone);
      window.clear();
      window_index.clear();
      window_records = 0;
      window_bytes = 0;
      return replayed;
    };
    LogRecord log;
    while (offset < filesize) {
      collect_progress.Tick(offset);
      const LogReadResult read = ReadLogDetailed(offset, &log);
      if (read.status == LogReadStatus::kUnavailable) {
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (read.status != LogReadStatus::kComplete) {
        if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
          offset = *next;
          continue;
        }
        return StatusError(
            StatusCode::kCorrupt,
            "failed to read WAL record at LSN " + std::to_string(offset));
      }
      if (IsPageManipulation(log.type)) {
        if (redo_pages.contains(log.pid) && offset >= rec_lsn_of(log.pid)) {
          const auto [it, inserted] =
              window_index.try_emplace(log.pid, window.size());
          if (inserted) {
            window.emplace_back(log.pid,
                                std::vector<std::pair<lsn_t, LogRecord>>{});
          }
          window[it->second].second.emplace_back(offset, log);
          ++window_records;
          window_bytes += log.Size();
        }
      } else if (log.type == LogType::kCommit) {
        // Redundant with the analysis pass (kept as a cheap safety net).
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
      if (window_records >= kRedoWindowRecords ||
          window_bytes >= kRedoWindowBytes) {
        RETURN_IF_FAIL(flush_window());
      }
    }
    RETURN_IF_FAIL(flush_window());
  }

  // Global loser UNDO (section 1.7): compensate every record on each loser's
  // prev_lsn chain that per-page replay could not see -- pages outside the
  // dirty page table or records below redo_start_point.
  RETURN_IF_FAIL(UndoLoserChains(
      loser_heads, &undone, tm, valid_end, scan_start, known_committed,
      &loser_compensation_evidence, &body_restore_anchors));

  page_id_t required_allocator_max = 0;
  for (const auto& [txn, pid] : max_pid_by_txn) {
    if (compensated_allocation_txns.contains(txn)) {
      continue;
    }
    const bool loser = !committed_txn.contains(txn);
    if (loser && v6_allocation_txns.contains(txn)) {
      continue;
    }
    required_allocator_max = std::max(required_allocator_max, pid);
  }

  ASSIGN_OR_RETURN(PageRef, meta,
                   pool_->GetPageForRecovery(kMetaPageId, nullptr));
  if (!meta->IsValid()) {
    meta->page_id = kMetaPageId;
    meta->page_lsn = 0;
    meta->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  auto& allocator = meta->body.meta_page;
  const page_id_t preserved_head = allocator.FirstFreePage();
  const page_id_t scan_high_water =
      std::max({allocator.MaxPageCount(), max_seen_pid, checkpoint_max_pid,
                preserved_head});
  std::unordered_set<page_id_t> preserved_free_pages;
  page_id_t preserved_tail = 0;
  bool preserved_chain_valid = preserved_head != 0;
  for (page_id_t cursor = preserved_head;
       preserved_chain_valid && cursor != 0;) {
    if (cursor > scan_high_water ||
        preserved_free_pages.size() > scan_high_water ||
        preserved_free_pages.contains(cursor)) {
      preserved_chain_valid = false;
      break;
    }
    ASSIGN_OR_RETURN(PageRef, preserved,
                     pool_->GetPageForRecovery(cursor, nullptr));
    if (preserved->Type() != PageType::kFreePage ||
        (!preserved->IsValid() && preserved->checksum != 0)) {
      preserved_chain_valid = false;
      break;
    }
    preserved_free_pages.insert(cursor);
    preserved_tail = cursor;
    cursor = preserved->body.free_page.NextFreePage();
  }
  if (!preserved_chain_valid) {
    preserved_free_pages.clear();
    preserved_tail = 0;
  }

  page_id_t free_head = 0;
  page_id_t highest_live_page = 0;
  page_id_t highest_free_page = 0;
  size_t rebuilt_free_pages = 0;
  // Torn-page sweep before the free-list rebuild. The DPT only names the pages
  // the last checkpoint still needed, so a page that was already flushed clean
  // (and therefore dropped from the DPT) has no redo entry: if the crash tore
  // it, nothing above noticed, and the first reader to touch it gets an empty
  // PageRef from PageManager::GetPage -- BPlusTree::Open then bootstraps a
  // fresh tree and the whole catalog silently disappears. Detect torn images
  // by checksum over the whole allocated range (the same range this function
  // already walks) instead of by DPT membership, and rebuild each from the
  // retained WAL.
  //
  // A page whose stored checksum is still 0 was never written (sparse tail),
  // and a free-typed page holds no data: both keep the historical skip
  // behaviour. Everything else must come back, or the open fails closed
  // instead of losing committed state.
  // Classify the whole allocated range in parallel: the sweep pread()s every
  // page below the high-water mark, which on a large database dominates
  // restart time (the DPT only names pages dirty at the last checkpoint).
  // Workers only READ and checksum pages; the SPR for anything broken runs
  // afterwards on this thread in ascending page order, because it re-enters
  // the allocator and must not race other page fetches.
  std::vector<page_id_t> sweep_broken;
  {
    RecoveryProgressLogger sweep_progress("torn-page sweep", 1,
                                          scan_high_water + 1);
    std::mutex broken_mu;
    auto classify_page = [&](page_id_t pid) -> Status {
      // A broken on-disk image must reach the SPR pass verbatim, so this
      // bypasses PagePool checksum enforcement (same as the DPT pass above).
      ASSIGN_OR_RETURN(PageRef, probe, pool_->GetPageForRecovery(pid, nullptr));
      if (probe->IsValid() || probe->checksum == 0 ||
          probe->Type() == PageType::kFreePage) {
        return Status::kSuccess;
      }
      std::scoped_lock lock(broken_mu);
      sweep_broken.push_back(pid);
      return Status::kSuccess;
    };
    const size_t sweep_workers = RecoveryWorkerCount(scan_high_water);
    if (sweep_workers <= 1) {
      for (page_id_t pid = 1; pid <= scan_high_water; ++pid) {
        sweep_progress.Tick(pid);
        RETURN_IF_FAIL(classify_page(pid));
      }
    } else {
      std::atomic<page_id_t> cursor{1};
      std::mutex error_mu;
      Status first_error = Status::kSuccess;
      auto sweep_worker = [&]() {
        for (;;) {
          const page_id_t pid = cursor.fetch_add(1, std::memory_order_relaxed);
          if (pid > scan_high_water || first_error != Status::kSuccess) {
            return;
          }
          sweep_progress.Tick(pid);
          Status status = Status::kSuccess;
          try {
            status = classify_page(pid);
          } catch (const std::exception& ex) {
            status = StatusError(StatusCode::kRuntimeError, ex.what());
          } catch (...) {
            status = StatusError(StatusCode::kRuntimeError,
                                 "unknown torn-page sweep failure");
          }
          if (status != Status::kSuccess) {
            std::scoped_lock lock(error_mu);
            if (first_error == Status::kSuccess) {
              first_error = status;
            }
            cursor.store(scan_high_water + 1, std::memory_order_relaxed);
            return;
          }
        }
      };
      std::vector<std::jthread> threads;
      threads.reserve(sweep_workers);
      for (size_t i = 0; i < sweep_workers; ++i) {
        try {
          threads.emplace_back(sweep_worker);
        } catch (const std::exception& ex) {
          std::scoped_lock lock(error_mu);
          first_error = StatusError(StatusCode::kRuntimeError, ex.what());
          cursor.store(scan_high_water + 1, std::memory_order_relaxed);
          break;
        } catch (...) {
          std::scoped_lock lock(error_mu);
          first_error = StatusError(StatusCode::kRuntimeError,
                                    "failed to start torn-page sweep worker");
          cursor.store(scan_high_water + 1, std::memory_order_relaxed);
          break;
        }
      }
      for (std::jthread& thread : threads) {
        thread.join();
      }
      RETURN_IF_FAIL(first_error);
    }
  }
  std::ranges::sort(sweep_broken);
  for (const page_id_t pid : sweep_broken) {
    LOG(WARN) << "Page " << pid
              << " is torn outside the dirty page table; rebuilding from WAL";
    ASSIGN_OR_RETURN(PageRef, probe, pool_->GetPageForRecovery(pid, nullptr));
    RETURN_IF_FAIL(SinglePageRecovery(std::move(probe), tm, &undone, scan_start,
                                      valid_end, known_committed));
  }
  for (page_id_t pid = scan_high_water; 1 <= pid; --pid) {
    ASSIGN_OR_RETURN(PageRef, p, pool_->GetPageForRecovery(pid, nullptr));
    if (p->Type() != PageType::kFreePage) {
      highest_live_page = std::max(highest_live_page, pid);
      continue;
    }
    if (!p->IsValid() && p->checksum != 0) {
      LOG(WARN) << "free-list rebuild skips corrupt page " << pid;
      continue;
    }
    // The descending order makes the FIRST linked page the highest free
    // page; the allocator high-water below must cover it (see the comment
    // before RestoreMaxPageCount).
    if (highest_free_page == 0) {
      highest_free_page = pid;
    }
    if (preserved_free_pages.contains(pid)) {
      ++rebuilt_free_pages;
      continue;
    }
    p.GetFreePage().SetNextFreePage(free_head);
    // The relink rewrote the body; keep IsValid() meaning "intact" while the
    // page stays dirty, so the rebuilt free list is actually persisted.
    p->MarkRepairedInMemory();
    free_head = pid;
    ++rebuilt_free_pages;
  }
  if (!preserved_free_pages.empty()) {
    ASSIGN_OR_RETURN(PageRef, tail,
                     pool_->GetPageForRecovery(preserved_tail, nullptr));
    tail.GetFreePage().SetNextFreePage(free_head);
    tail->MarkRepairedInMemory();
    free_head = preserved_head;
  }
  required_allocator_max = std::max(required_allocator_max, highest_live_page);
  // The WAL evidence for the high-water mark is deliberately partial (loser
  // and compensated allocations are excluded), but the FREE-LIST evidence is
  // direct: a rebuilt list that reaches above the restored high-water (e.g.
  // a committed destroy whose allocating transaction's evidence was
  // excluded) hands out a page whose recycled-allocation undo later fails
  // the payload self-check (recycled_next above old_max_page_count), turning
  // every subsequent abort of such a transaction into a spurious kCorrupt
  // fail-stop. Raise the high-water to cover the list; a monotone high-water
  // is always safe (it only bounds page ids, the free list governs reuse).
  required_allocator_max =
      std::max({required_allocator_max, highest_free_page, preserved_tail});
  if (allocator.MaxPageCount() < required_allocator_max) {
    LOG(INFO) << "Restoring meta allocator high-water mark: "
              << allocator.MaxPageCount() << " -> " << required_allocator_max;
    allocator.RestoreMaxPageCount(required_allocator_max);
  }
  allocator.RebuildFreeListHead(free_head);
  // The allocator fields above rewrote the meta page body; mark it repaired so
  // InitializeMetaPage() (and every later reader) sees an intact page rather
  // than a false "broken" verdict, while it stays dirty until it is flushed.
  meta->MarkRepairedInMemory();
  if (rebuilt_free_pages != 0) {
    LOG(INFO) << "Rebuilt free list: " << rebuilt_free_pages
              << " pages, head=" << free_head;
  }

  // Stale routing-link sweeps (separator + foster). These run at the very end
  // of recovery -- after UndoLoserChains, after the torn-page sweep's SPR
  // rebuilds, and after the free-list rebuild -- because only then is a
  // page's liveness final: a loser undo whose physical half was skipped
  // (CLR evidence / page-incarnation type rules) can leave a live separator
  // or foster link naming a child the same recovery frees, and a body-
  // restoring SPR anchor applied after the undo pass can resurrect an
  // already-cleared link. A link that survives to this point with a free
  // child routes future descents into a free page: the grow sibling can be
  // allocated from that pid and latch the same page twice (EDEADLK wedge),
  // and with latching set aside the tree would double-route one page id.
  // Absorb a dead link: CLR first so replays stay idempotent, then remove
  // the physical link. Only an exactly-matching, exactly-dead link
  // qualifies: a link whose child is still live (re-issued, or a split that
  // legitimately raced it) stays untouched.
  if (tm != nullptr &&
      std::getenv("TINYLAMB_DISABLE_SEPARATOR_SWEEP") == nullptr) {
    size_t swept_separators = 0;
    for (const LoserBranchInsert& pending : loser_branch_inserts) {
      if (committed_txn.contains(pending.txn)) {
        continue;
      }
      ASSIGN_OR_RETURN(PageRef, child,
                       pool_->GetPageForRecovery(pending.child, nullptr));
      if (!child->IsValid() || child->Type() != PageType::kFreePage) {
        continue;
      }
      ASSIGN_OR_RETURN(PageRef, parent,
                       pool_->GetPageForRecovery(pending.parent, nullptr));
      if (!parent->IsValid() || parent->Type() != PageType::kBranchPage) {
        continue;
      }
      LogRecord original;
      if (!ReadLog(pending.lsn, &original) ||
          original.type != LogType::kInsertBranch ||
          original.txn_id != pending.txn ||
          original.redo_page != pending.child) {
        continue;
      }
      BranchPage& branch = parent->body.branch_page;
      const int pos = branch.Search(original.key, false);
      if (pos < 0 || std::cmp_greater_equal(pos, branch.RowCount()) ||
          branch.GetKey(static_cast<size_t>(pos)) != original.key ||
          branch.GetValue(static_cast<size_t>(pos)) != pending.child) {
        continue;
      }
      ASSIGN_OR_RETURN(
          lsn_t, clr_end,
          tm->CompensateInsertBranchLog(pending.txn, pending.parent,
                                        original.key, original.prev_lsn));
      parent->DeleteBranchImpl(original.key);
      parent->SetRecLSN(pending.lsn);
      parent->SetPageLSN(std::max(parent->PageLSN(), clr_end));
      parent->MarkRepairedInMemory();
      ++swept_separators;
    }

    // The foster twin: the link lives on the splitting page itself (a leaf
    // root's pending height growth), so the parent can be a leaf.
    size_t swept_fosters = 0;
    for (const LoserFosterSet& pending : loser_foster_sets) {
      if (committed_txn.contains(pending.txn)) {
        continue;
      }
      ASSIGN_OR_RETURN(PageRef, child,
                       pool_->GetPageForRecovery(pending.child, nullptr));
      if (!child->IsValid() || child->Type() != PageType::kFreePage) {
        continue;
      }
      ASSIGN_OR_RETURN(PageRef, parent,
                       pool_->GetPageForRecovery(pending.parent, nullptr));
      if (!parent->IsValid()) {
        continue;
      }
      StatusOr<FosterPair> current(Status::kNotExists);
      if (parent->Type() == PageType::kLeafPage) {
        current = parent->body.leaf_page.GetFoster();
      } else if (parent->Type() == PageType::kBranchPage) {
        current = parent->body.branch_page.GetFoster();
      } else {
        continue;
      }
      if (!current.HasValue() || current.Value().child_pid != pending.child) {
        continue;  // absorbed or re-split since; only a dead exact link sweeps
      }
      LogRecord original;
      if (!ReadLog(pending.lsn, &original) ||
          original.type != LogType::kSetFoster ||
          original.txn_id != pending.txn || original.pid != pending.parent) {
        continue;
      }
      ASSIGN_OR_RETURN(
          lsn_t, clr_end,
          tm->CompensateSetFosterLog(pending.txn, pending.parent,
                                     FosterPair("", 0), original.prev_lsn));
      RETURN_IF_FAIL(parent->SetFosterImpl(FosterPair("", 0)));
      parent->SetRecLSN(pending.lsn);
      parent->SetPageLSN(std::max(parent->PageLSN(), clr_end));
      parent->MarkRepairedInMemory();
      ++swept_fosters;
    }
    if (swept_separators > 0 || swept_fosters > 0) {
      LOG(WARN) << "swept " << swept_separators << " loser separator(s) and "
                << swept_fosters
                << " loser foster link(s) whose child page stayed free";
    }
  }
  // One-line postmortem: a crashed database's restart is judged from this
  // line (window replayed, losers rolled back, pages rebuilt, wall time).
  const auto recovery_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - recovery_started);
  LOG(INFO) << "Recovery complete: WAL window [" << scan_start << ", "
            << valid_end << "), " << committed_txn.size()
            << " committed transaction(s), " << loser_heads.size()
            << " loser chain(s) rolled back, "
            << (broken_pages.size() + sweep_broken.size())
            << " page(s) rebuilt from WAL, free list holds "
            << rebuilt_free_pages << " page(s), took " << recovery_ms.count()
            << " ms";
  return Status::kSuccess;
}

lsn_t RecoveryManager::ValidLogEnd(lsn_t from) const {
  const auto status = ValidLogEndStatus(from);
  return status.HasValue() ? status.Value() : 0;
}

StatusOr<lsn_t> RecoveryManager::ValidLogEndStatus(
    lsn_t from, const std::function<void(lsn_t)>& progress) const {
  ASSIGN_OR_RETURN(lsn_t, filesize, LogEndStatus());
  ASSIGN_OR_RETURN(lsn_t, log_start, LogStartStatus());
  lsn_t offset = std::max(from, log_start);
  LogRecord log;
  while (offset < filesize) {
    if (progress) {
      progress(offset);
    }
    const LogReadResult read = ReadLogDetailed(offset, &log);
    if (read.status == LogReadStatus::kUnavailable) {
      return StatusError(StatusCode::kIOError,
                         "WAL read failed at LSN " + std::to_string(offset));
    }
    if (read.status != LogReadStatus::kComplete) {
      if (const auto next = PreRollPaddingEnd(offset); next.has_value()) {
        offset = *next;
        continue;
      }
      return offset;
    }
    if (log.Size() == 0) {
      return offset;
    }
    offset += log.Size();
  }
  return offset;
}

RecoveryManager::TailDamage RecoveryManager::ClassifyTailDamage(
    lsn_t valid_end, lsn_t on_disk) const {
  if (valid_end >= on_disk) {
    return TailDamage::kNone;
  }
  bool any = false;
  auto scanned = Logger::FindHighestSegment(log_name_, &any);
  if (!scanned.HasValue() || !any ||
      Logger::SegmentIndex(valid_end, segment_size_) != scanned.Value()) {
    return TailDamage::kSuspect;
  }

  constexpr size_t kZeroProbe = 512;
  std::array<char, kZeroProbe> probe{};
  bool all_zero = true;
  for (lsn_t at = valid_end; at < on_disk; at += kZeroProbe) {
    const size_t want =
        static_cast<size_t>(std::min<lsn_t>(kZeroProbe, on_disk - at));
    const ReadAtResult read = ReadAtDetailed(at, probe.data(), want);
    if (read.status == ReadAtStatus::kUnavailable ||
        read.status == ReadAtStatus::kEof ||
        read.status == ReadAtStatus::kShort) {
      return TailDamage::kSuspect;
    }
    if (std::any_of(probe.begin(),
                    probe.begin() + static_cast<ptrdiff_t>(read.bytes),
                    [](char byte) { return byte != 0; })) {
      all_zero = false;
      break;
    }
    if (read.status == ReadAtStatus::kTruncated || read.bytes != want) {
      break;
    }
  }
  if (all_zero) {
    const lsn_t segment_end =
        (static_cast<lsn_t>(Logger::SegmentIndex(valid_end, segment_size_)) +
         1) *
        static_cast<lsn_t>(segment_size_);
    return on_disk >= segment_end ? TailDamage::kSuspect
                                  : TailDamage::kZeroTail;
  }

  LogRecord probe_record;
  const LogReadResult record_read = ReadLogDetailed(valid_end, &probe_record);
  if (record_read.status != LogReadStatus::kShort) {
    return TailDamage::kSuspect;
  }
  constexpr size_t kHeaderSize = (sizeof(uint32_t) * 2) + sizeof(uint16_t);
  std::array<char, kHeaderSize> header{};
  const ReadAtResult header_read =
      ReadAtDetailed(valid_end, header.data(), header.size());
  if (header_read.status == ReadAtStatus::kUnavailable ||
      header_read.bytes == 0) {
    return TailDamage::kSuspect;
  }
  if (header_read.bytes < sizeof(uint32_t)) {
    std::array<char, sizeof(uint32_t)> magic_bytes{};
    SerializeU32(magic_bytes.data(), kSerdesMagic);
    for (size_t i = 0; i < header_read.bytes; ++i) {
      if (header[i] != magic_bytes[i]) {
        return TailDamage::kSuspect;
      }
    }
    return TailDamage::kTornRecord;
  }
  uint32_t magic = 0;
  DeserializeU32(header.data(), &magic);
  if (magic != kSerdesMagic) {
    return TailDamage::kSuspect;
  }
  if (header_read.bytes >= sizeof(uint32_t) * 2) {
    uint32_t version = 0;
    DeserializeU32(header.data() + sizeof(uint32_t), &version);
    if (version < kLegacyWalRecordVersion || version > kWalRecordVersion) {
      return TailDamage::kSuspect;
    }
  }
  if (header_read.bytes >= kHeaderSize) {
    uint16_t raw_type = 0;
    DeserializeU16(header.data() + (sizeof(uint32_t) * 2), &raw_type);
    if (!IsKnownLogType(static_cast<LogType>(raw_type))) {
      return TailDamage::kSuspect;
    }
  }
  return TailDamage::kTornRecord;
}

RecoveryManager::LogReadResult RecoveryManager::ReadLogDetailed(
    lsn_t lsn, LogRecord* dst) const {
  dst->Clear();
  constexpr size_t kHeaderSize = (sizeof(uint32_t) * 2) + sizeof(uint16_t);
  std::array<char, kHeaderSize> header{};
  const ReadAtResult header_read =
      ReadAtDetailed(lsn, header.data(), header.size());
  if (header_read.status != ReadAtStatus::kComplete) {
    if (header_read.status == ReadAtStatus::kUnavailable) {
      return {.status = LogReadStatus::kUnavailable};
    }
    if (header_read.status == ReadAtStatus::kEof) {
      return {.status = LogReadStatus::kEof};
    }
    return {.status = header_read.status == ReadAtStatus::kTruncated
                          ? LogReadStatus::kShort
                          : LogReadStatus::kCorrupt};
  }
  uint32_t magic = 0;
  uint32_t version = 0;
  uint16_t raw_type = 0;
  DeserializeU32(header.data(), &magic);
  DeserializeU32(header.data() + sizeof(uint32_t), &version);
  DeserializeU16(header.data() + (sizeof(uint32_t) * 2), &raw_type);
  if (magic != kSerdesMagic ||
      (version < kLegacyWalRecordVersion || version > kWalRecordVersion) ||
      !IsKnownLogType(static_cast<LogType>(raw_type))) {
    return {.status = LogReadStatus::kCorrupt};
  }

  constexpr size_t kInitialWindow = 4096;
  constexpr size_t kMaxWindow = 16U << 20;
  // The live logger's buffered end bounds what any medium can serve: the
  // ring declines ranges past it and the segment files have not received
  // those bytes yet, so probing the default window past it would
  // manufacture kShort/kEof for records that are fully readable (a runtime
  // undo walk reads the newest records -- exactly the ones near that end).
  // Clamp the growing probe to it; restart-time reads see
  // buffered == file end and are unaffected.
  const lsn_t readable_end = logger_ != nullptr
                                 ? logger_->BufferedLSN()
                                 : std::numeric_limits<lsn_t>::max();
  for (size_t want = kInitialWindow;; want *= 4) {
    const size_t probe =
        readable_end > lsn ? std::min(want, readable_end - lsn) : 0;
    std::string buffer(probe, '\0');
    const ReadAtResult read = ReadAtDetailed(lsn, buffer.data(), probe);
    if (read.status == ReadAtStatus::kUnavailable) {
      return {.status = LogReadStatus::kUnavailable};
    }
    if (read.bytes < kHeaderSize) {
      if (read.status == ReadAtStatus::kEof) {
        return {.status = LogReadStatus::kEof};
      }
      return {.status = read.status == ReadAtStatus::kTruncated
                            ? LogReadStatus::kShort
                            : LogReadStatus::kCorrupt};
    }
    buffer.resize(read.bytes);
    std::istringstream in;
    in.str(buffer);
    Decoder dec(in);
    dec >> *dst;
    if (dec.Failed() || !in.good() || !IsKnownLogType(dst->type)) {
      if (read.status != ReadAtStatus::kComplete) {
        if (read.status == ReadAtStatus::kEof) {
          return {.status = LogReadStatus::kEof};
        }
        return {.status = read.status == ReadAtStatus::kTruncated
                              ? LogReadStatus::kShort
                              : LogReadStatus::kCorrupt};
      }
      if (read.bytes == want && want < kMaxWindow) {
        continue;
      }
      return {.status = LogReadStatus::kCorrupt};
    }

    const size_t total = dst->Size();
    if (total == 0 || total > read.bytes) {
      if (read.status != ReadAtStatus::kComplete) {
        if (read.status == ReadAtStatus::kEof) {
          return {.status = LogReadStatus::kEof};
        }
        return {.status = read.status == ReadAtStatus::kTruncated
                              ? LogReadStatus::kShort
                              : LogReadStatus::kCorrupt};
      }
      if (read.bytes == want && want < kMaxWindow) {
        continue;
      }
      return {.status = LogReadStatus::kCorrupt};
    }
    if (dst->wire_version >= kCrcWalRecordVersion) {
      if (total < kWalRecordCrcSize) {
        return {.status = LogReadStatus::kCorrupt};
      }
      const size_t body = total - kWalRecordCrcSize;
      const char* crc_pos = buffer.data() + body;
      const uint32_t stored =
          (static_cast<uint32_t>(static_cast<unsigned char>(crc_pos[0]))
           << 24) |
          (static_cast<uint32_t>(static_cast<unsigned char>(crc_pos[1]))
           << 16) |
          (static_cast<uint32_t>(static_cast<unsigned char>(crc_pos[2])) << 8) |
          static_cast<uint32_t>(static_cast<unsigned char>(crc_pos[3]));
      const uint32_t computed = Crc32C(buffer.data(), body);
      if (stored != computed) {
        LOG(WARN) << "WAL CRC mismatch at LSN " << lsn << " (stored " << stored
                  << ", computed " << computed << ")";
        return {.status = LogReadStatus::kCorrupt};
      }
    }
    return {.status = LogReadStatus::kComplete};
  }
}

bool RecoveryManager::ReadLog(lsn_t lsn, LogRecord* dst) const {
  return ReadLogDetailed(lsn, dst).status == LogReadStatus::kComplete;
}

uint8_t RecoveryManager::DebugReadStatusFor(lsn_t lsn) const {
  LogRecord scratch;
  return static_cast<uint8_t>(ReadLogDetailed(lsn, &scratch).status);
}

Status RecoveryManager::UndoLoserChains(
    const std::vector<lsn_t>& loser_heads, UndoneRecorder* undone,
    TransactionManager* tm, lsn_t scan_end, lsn_t scan_start,
    const std::unordered_set<txn_id_t>* known_committed,
    const std::map<std::tuple<txn_id_t, int, page_id_t, std::string>,
                   std::pair<lsn_t, lsn_t>>* compensations,
    const std::unordered_map<page_id_t, lsn_t>* body_restore_anchors) {
  const int stop_after = TestUndoStopAfter();
  int applied = 0;
  // Pages whose loser allocation this walk wants to free while the page
  // still holds live entries, kept as (LSN, allocation record). Their frees
  // are retried after every chain finished -- see the rehome pass below.
  std::vector<std::pair<lsn_t, LogRecord>> deferred_frees;
  // In-place root growth restored by this walk: each is a page whose
  // body-restore rewrites the full pre-growth body, so it is the settled
  // home for entries stranded on pages the same rollback frees.
  std::unordered_set<page_id_t> spines;
  for (lsn_t head : loser_heads) {
    if (head < scan_start) {
      return StatusError(StatusCode::kCorrupt,
                         "loser undo head is below retained WAL start");
    }
    std::unordered_set<lsn_t> visited;
    txn_id_t expected_txn = 0;
    lsn_t cur = head;
    size_t chain_records = 0;
    while (cur >= scan_start) {
      if (!visited.insert(cur).second) {
        return StatusError(
            StatusCode::kCorrupt,
            "cycle in loser undo chain at LSN " + std::to_string(cur));
      }
      LogRecord log;
      if (!ReadLog(cur, &log)) {
        return StatusError(
            StatusCode::kCorrupt,
            "unreadable loser undo record at LSN " + std::to_string(cur));
      }
      if (expected_txn == 0) {
        expected_txn = log.txn_id;
        // Name the loser up front: an operator triaging a restart wants the
        // victim's identity (and chain size) without re-deriving it from the
        // per-record WARNs that follow.
        LOG(INFO) << "Rolling back loser transaction " << expected_txn
                  << " (chain head LSN " << head << ")";
      } else if (log.txn_id != expected_txn) {
        return StatusError(StatusCode::kCorrupt,
                           "loser undo chain crossed transaction ids");
      }
      if (cur == 0 && log.prev_lsn != 0) {
        return StatusError(StatusCode::kCorrupt,
                           "LSN 0 has a nonzero previous LSN for transaction " +
                               std::to_string(log.txn_id));
      }
      if (log.type != LogType::kFullPageImage && IsPageManipulation(log.type) &&
          !undone->Contains(cur)) {
        ASSIGN_OR_RETURN(PageRef, target,
                         pool_->GetPageForRecovery(log.pid, nullptr));
        if (!target->IsValid()) {
          LOG(INFO) << "Loser undo rebuilds broken page " << log.pid;
          RETURN_IF_FAIL(SinglePageRecovery(std::move(target), tm, undone,
                                            scan_start, scan_end,
                                            known_committed));
        } else {
          // A kCompensate* record in the window with this operation's
          // identity proves an EARLIER recovery pass already undid it:
          // re-applying the physical mutation would corrupt the page's
          // current incarnation (the stale-undo twin of what PageReplay's
          // compensated_ops prevents per page -- session-fuzzer: a second
          // restart re-freed a page a committed allocation had re-issued).
          // The page_lsn re-check keeps the skip honest for a crash BETWEEN
          // the earlier pass's CLR append and its physical mutation: only a
          // page stamped at or above that CLR's end carries the undo's
          // effects, so anything less still gets re-undone here.
          bool stale_undo = false;
          if (compensations != nullptr &&
              !CompensatedOpType(log.type).has_value()) {
            const auto it =
                compensations->find({log.txn_id, static_cast<int>(log.type),
                                     log.pid, UndoOpId(log)});
            if (it != compensations->end() && it->second.first > cur &&
                target->PageLSN() >= it->second.second) {
              stale_undo = true;
              // A body-restoring anchor replayed by THIS pass's torn-page
              // rebuild resurrected content from before the closing CLR:
              // the evidence no longer proves the undo is materialized.
              // Re-attempt it. Only content records take this path -- the
              // kSystemAllocPage undo below never consults a voided
              // stale_undo, so a page a committed allocation re-issued can
              // still not be re-freed -- and LogUndo's per-record type check
              // refuses to mutate a recycled incarnation.
              if (body_restore_anchors != nullptr &&
                  std::getenv("TINYLAMB_DISABLE_ANCHOR_VOID") == nullptr) {
                const auto anchor = body_restore_anchors->find(log.pid);
                if (anchor != body_restore_anchors->end() &&
                    it->second.first < anchor->second) {
                  stale_undo = false;
                }
              }
            }
          }
          const bool allocation_undo =
              log.type == LogType::kSystemAllocPage &&
              log.wire_version >= kAllocationPayloadWalVersion;
          // A page-free whose page still holds live entries would strand
          // them (see HasLiveEntries): defer the free to the rehome pass at
          // the end of the walk. Only the loser's own incarnation qualifies
          // -- a page re-issued to a committed allocation must take the
          // normal stale path below instead.
          if (allocation_undo &&
              log.allocation_kind != PageAllocationKind::kInPlace &&
              target->Type() == log.allocated_page_type &&
              HasLiveEntries(target)) {
            deferred_frees.emplace_back(cur, log);
          } else if (!stale_undo && (allocation_undo ||
                                     target->PageLSN() >= cur + log.Size())) {
            RETURN_IF_FAIL(LogUndo(target, cur, log, tm, pool_).GetStatus());
            if (allocation_undo &&
                log.allocation_kind == PageAllocationKind::kInPlace) {
              spines.insert(log.pid);
            }
            undone->Record(cur);
            if (stop_after > 0 && ++applied >= stop_after) {
              LOG(WARN) << "test hook: loser undo stopped after " << applied
                        << " application(s)";
              return Status::kSuccess;
            }
          } else if (stale_undo) {
            undone->Record(cur);
          }
        }
      }
      if (cur == 0) {
        break;
      }
      std::optional<lsn_t> next;
      if (log.prev_lsn != 0) {
        next = log.prev_lsn;
      } else if (scan_start == 0) {
        LogRecord first;
        if (!ReadLog(0, &first)) {
          return StatusError(
              StatusCode::kCorrupt,
              "unreadable first record at LSN 0 for transaction " +
                  std::to_string(log.txn_id));
        }
        if (first.txn_id == log.txn_id) {
          next = 0;
        }
      }
      if (!next.has_value()) {
        break;
      }
      if (*next >= cur || *next < scan_start) {
        return StatusError(StatusCode::kCorrupt,
                           "loser undo chain points outside its history");
      }
      cur = *next;
      ++chain_records;
    }
    LOG(INFO) << "Loser transaction " << expected_txn << " rolled back ("
              << chain_records << " chain record(s) walked)";
  }
  // Deferred page-frees, retried after every chain finished. All
  // body-restoring undos (in-place growth restores rewrite a collapsing
  // root's full body) have run by now, so an entry re-homed below can no
  // longer be clobbered by a later memcpy. Each page is re-inserted into
  // the settled tree (CLR first, so replays stay idempotent) before its
  // free finally runs; a crash between the two halves replays into
  // kDuplicates, which the insert below treats as done.
  for (const auto& [free_lsn, alloc_log] : deferred_frees) {
    ASSIGN_OR_RETURN(PageRef, page,
                     pool_->GetPageForRecovery(alloc_log.pid, nullptr));
    if (!page->IsValid() || !HasLiveEntries(page)) {
      // Collapsed to empty by the remaining (older) undos: plain free.
      RETURN_IF_FAIL(LogUndo(page, free_lsn, alloc_log, tm, pool_).GetStatus());
      undone->Record(free_lsn);
      continue;
    }
    if (page->Type() != PageType::kLeafPage || spines.empty()) {
      LOG(WARN) << "undo keeps a page whose free would strand live content: "
                   "page "
                << alloc_log.pid << " type " << static_cast<int>(page->Type())
                << " entries="
                << (page->Type() == PageType::kLeafPage
                        ? page->body.leaf_page.RowCount()
                        : 0)
                << " spine_candidates=" << spines.size();
      continue;
    }
    LeafPage& leaf = page->body.leaf_page;
    std::vector<std::pair<std::string, std::string>> entries;
    entries.reserve(leaf.RowCount());
    for (slot_t i = 0; i < leaf.RowCount(); ++i) {
      // Copies: the views point into a body the free below re-initializes.
      entries.emplace_back(leaf.GetKey(i), leaf.GetValue(i));
    }
    page_id_t home = 0;
    if (spines.size() == 1) {
      home = *spines.begin();
    } else {
      for (const page_id_t candidate : spines) {
        ASSIGN_OR_RETURN(PageRef, spine_page,
                         pool_->GetPageForRecovery(candidate, nullptr));
        if (spine_page->IsValid() &&
            spine_page->Type() == PageType::kLeafPage) {
          const IndexKey key(std::string_view(entries.front().first));
          const IndexKey low = spine_page->body.leaf_page.GetLowFence();
          const IndexKey high = spine_page->body.leaf_page.GetHighFence();
          if (low < key && key < high) {
            home = candidate;
            break;
          }
        }
      }
    }
    ASSIGN_OR_RETURN(PageRef, home_page,
                     pool_->GetPageForRecovery(home, nullptr));
    if (home == 0 || !home_page->IsValid() ||
        home_page->Type() != PageType::kLeafPage) {
      LOG(WARN) << "undo keeps leaf " << alloc_log.pid
                << ": no settled home found for " << entries.size()
                << " live entries (spine_candidates=" << spines.size() << ")";
      continue;
    }
    bool rehomed_all = true;
    for (const auto& [key, value] : entries) {
      ASSIGN_OR_RETURN(
          lsn_t, clr_end,
          tm->CompensateDeleteLog(alloc_log.txn_id, home, key, value, 0));
      const Status inserted = home_page->InsertImpl(key, value);
      if (inserted == Status::kDuplicates) {
        continue;  // a crash between the CLR and the free already re-homed it
      }
      if (inserted != Status::kSuccess) {
        LOG(WARN) << "undo re-home insert failed (page " << home
                  << " keeps the entries): key_size=" << key.size() << ": "
                  << inserted;
        rehomed_all = false;
        break;
      }
      home_page->SetRecLSN(clr_end);
      home_page->SetPageLSN(std::max(home_page->PageLSN(), clr_end));
      home_page->MarkRepairedInMemory();
    }
    if (rehomed_all) {
      LOG(WARN) << "undo re-homed " << entries.size() << " live entr"
                << (entries.size() == 1 ? "y" : "ies") << " from leaf "
                << alloc_log.pid << " into settled leaf " << home
                << " before freeing it";
      RETURN_IF_FAIL(LogUndo(page, free_lsn, alloc_log, tm, pool_).GetStatus());
      undone->Record(free_lsn);
    }
  }
  return Status::kSuccess;
}

Status RecoveryManager::LogUndoWithPage(lsn_t lsn, const LogRecord& log,
                                        TransactionManager* tm) {
  if (log.type == LogType::kUnknown) {
    return StatusError(StatusCode::kCorrupt, "unknown log type");
  }
  if (IsPageManipulation(log.type)) {
    ASSIGN_OR_RETURN(PageRef, target, pool_->GetPage(log.pid));
    ASSIGN_OR_RETURN(lsn_t, clr_end, LogUndo(target, lsn, log, tm, pool_));
    // Runtime undo: the page image now reflects the CLR, which sits at the
    // log tail ABOVE the undone record.  Leaving the stamp at the undone
    // record's end would regress page_lsn below the CLR and let the
    // write-back durability gate (DurableLSN >= page_lsn) release the page
    // while its compensating record is still volatile.  No record for this
    // page can land between the two stamps: the page latch is held for the
    // whole undo, and every later record exceeds the CLR.
    const lsn_t stamp = std::max(lsn + log.Size(), clr_end);
    target->SetRecLSN(lsn);
    target->SetPageLSN(stamp);
  }
  return Status::kSuccess;
}

}  // namespace tinylamb
