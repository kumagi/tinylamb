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

#ifndef TINYLAMB_RECOVERY_MANAGER_HPP
#define TINYLAMB_RECOVERY_MANAGER_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class Page;
class PageManager;
class PagePool;
class PageRef;
class Logger;
class Transaction;
class TransactionManager;
struct LogRecord;

class RecoveryManager {
 public:
  RecoveryManager(RecoveryManager&&) = delete;
  RecoveryManager& operator=(RecoveryManager&&) = delete;
  // LSNs already compensated during the current recovery. Shared by the
  // parallel per-page replay and the global loser-chain pass so an undo is
  // applied exactly once.
  class UndoneRecorder {
   public:
    void Record(lsn_t lsn) {
      std::scoped_lock lock(mu_);
      lsns_.insert(lsn);
    }
    [[nodiscard]] bool Contains(lsn_t lsn) {
      std::scoped_lock lock(mu_);
      return lsns_.contains(lsn);
    }

   private:
    std::mutex mu_;
    std::unordered_set<lsn_t> lsns_;
  };

  // Corruption policy: by default a torn/unparsable WAL tail is treated as
  // unrecoverable damage -- boot logs the offset and aborts. When enabled
  // (e.g. the `--force` CLI flag), recovery truncates the damaged tail and
  // replays the intact prefix instead. Independently of the flag, damage
  // that PROVABLY sits in the never-acknowledged tail (a record cut short by
  // the crash, or a zero-filled never-written region) is truncated
  // automatically so a crashed database boots without an operator; see
  // ClassifyTailDamage.
  static void SetTornTailTruncationAllowed(bool allowed) {
    torn_tail_truncation_allowed_.store(allowed, std::memory_order_relaxed);
  }
  [[nodiscard]] static bool TornTailTruncationAllowed() {
    return torn_tail_truncation_allowed_.load(std::memory_order_relaxed);
  }

  RecoveryManager(std::string_view log_path, PagePool* pp,
                  size_t segment_size = size_t{16} << 20);
  ~RecoveryManager();
  RecoveryManager(const RecoveryManager&) = delete;
  RecoveryManager& operator=(const RecoveryManager&) = delete;

  // Optional link to the live logger. When set, WAL reads first try the
  // logger's in-memory ring buffer (Logger::TryCopyBuffered) before falling
  // back to the segment files, so a runtime undo walk can read back records
  // that are still volatile. TransactionManager::Abort relies on this to
  // skip its pre-undo durability wait. Absent (or null, as in standalone
  // recovery setups) every read goes to the files exactly as before.
  void SetLogger(const Logger* logger) { logger_ = logger; }

  Status SinglePageRecovery(
      PageRef&& page, TransactionManager* tm,
      UndoneRecorder* undone =
          nullptr);  // NOLINT(readability-make-member-function-const)

  Status RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  bool ReadLog(lsn_t lsn, LogRecord* dst) const;

  // Diagnostic-only: classify why a record cannot be read back (public so
  // runtime failure handlers can name the failure mode in their evidence).
  // The value is a LogReadStatus cast to its underlying type.
  [[nodiscard]] uint8_t DebugReadStatusFor(lsn_t lsn) const;

  // Lowest absolute LSN that still has a segment file on disk. After WAL
  // recycling this is > 0 and the history below it is gone; recovery windows
  // and torn-tail scans must never read below it.
  [[nodiscard]] lsn_t LogStart() const;
  // Highest absolute LSN with bytes on disk (end of the newest segment).
  [[nodiscard]] lsn_t LogEnd() const;

  // Offset of the first unparseable record at or after `from` (the single
  // torn-tail authority; see D9 in docs/design.md for the CRC extension).
  // |progress| (when set) receives the scan position from time to time so a
  // multi-gigabyte retained log can log forward progress while it runs.
  [[nodiscard]] lsn_t ValidLogEnd(lsn_t from) const;

  Status LogUndoWithPage(lsn_t lsn, const LogRecord& log,
                         TransactionManager* tm);

  friend std::ostream& operator<<(std::ostream& o, const RecoveryManager& rm) {
    o << "RecoveryManager(log=" << rm.log_name_ << ")";
    return o;
  }

 private:
  // Diagnosis of the unparsable region [valid_end, on_disk). kTornRecord --
  // a record whose bytes were cut by the crash (valid header prefix, tail
  // missing) at the end of the newest segment; kZeroTail -- only zero bytes
  // follow (never-written region after a lost append); kSuspect -- damage
  // that may hide committed history (mid-file garbage, missing middle
  // segment): truncation then requires the operator's --force.
  enum class TailDamage : uint8_t { kNone, kTornRecord, kZeroTail, kSuspect };
  enum class ReadAtStatus : uint8_t {
    kComplete,
    kTruncated,
    kShort,
    kEof,
    kUnavailable,
  };
  struct ReadAtResult {
    size_t bytes = 0;
    ReadAtStatus status = ReadAtStatus::kUnavailable;
  };
  enum class LogReadStatus : uint8_t {
    kComplete,
    kShort,
    kEof,
    kCorrupt,
    kUnavailable,
  };
  struct LogReadResult {
    LogReadStatus status = LogReadStatus::kUnavailable;
  };
  struct ReadFd {
    explicit ReadFd(int descriptor) : fd(descriptor) {}
    ~ReadFd();
    ReadFd(const ReadFd&) = delete;
    ReadFd& operator=(const ReadFd&) = delete;
    ReadFd(ReadFd&&) = delete;
    ReadFd& operator=(ReadFd&&) = delete;
    int fd;
  };
  [[nodiscard]] TailDamage ClassifyTailDamage(lsn_t valid_end,
                                              lsn_t on_disk) const;
  [[nodiscard]] ReadAtResult ReadAtDetailed(lsn_t lsn, char* dst,
                                            size_t len) const;
  [[nodiscard]] std::optional<lsn_t> PreRollPaddingEnd(lsn_t lsn) const;
  [[nodiscard]] size_t ReadAt(lsn_t lsn, char* dst, size_t len) const;
  [[nodiscard]] std::shared_ptr<ReadFd> SegmentFdFor(lsn_t lsn) const;
  [[nodiscard]] LogReadResult ReadLogDetailed(lsn_t lsn, LogRecord* dst) const;
  [[nodiscard]] StatusOr<lsn_t> LogStartStatus() const;
  [[nodiscard]] StatusOr<lsn_t> LogEndStatus() const;
  [[nodiscard]] StatusOr<lsn_t> ValidLogEndStatus(
      lsn_t from, const std::function<void(lsn_t)>& progress = {}) const;
  // Rebuilds a broken page over the WAL window [scan_start, scan_end).
  // `known_committed` seeds the loser classification for windows whose
  // commit records may sit below scan_start (recycled prefix); when null,
  // the window itself is scanned for commit records (full-history mode).
  Status SinglePageRecovery(
      PageRef&& page, TransactionManager* tm, UndoneRecorder* undone,
      lsn_t scan_start, lsn_t scan_end,
      const std::unordered_set<txn_id_t>* known_committed);
  // Rebuilds EVERY broken page over one shared pass of the WAL window
  // [scan_start, scan_end). Same per-page semantics as SinglePageRecovery
  // (page_lsn reset before the first batch, kCorrupt when a broken page has
  // no records), but the window is read ONCE instead of once per page: a
  // kill -9 under load tears every hot page, and the per-page scans turned
  // the restart into O(torn pages x window bytes) of reads.
  // |body_restore_anchors| receives, for every page whose replay was anchored
  // at a record whose redo RESTORES A FULL BODY (kFullPageImage,
  // kCompensateDestroyPage, or an in-place kCompensateAllocPage), the anchor
  // LSN. Such an anchor resurrects page content from BEFORE earlier
  // recovery passes' CLRs, so loser-undo evidence older than the anchor is
  // void (see UndoLoserChains).
  Status RecoverBrokenPages(
      const std::vector<page_id_t>& broken_ids, TransactionManager* tm,
      UndoneRecorder* undone, lsn_t scan_start, lsn_t scan_end,
      const std::unordered_set<txn_id_t>* known_committed,
      std::unordered_map<page_id_t, lsn_t>* body_restore_anchors = nullptr);
  // Walks each loser transaction's prev_lsn chain newest-first and applies
  // undo to every record not already compensated by the per-page replay.
  // |compensations| carries the WAL evidence of undo work done by EARLIER
  // recovery passes: a chain's CLR threading only redirects past operations
  // the CURRENT pass can see on the chain, so without this a re-walk
  // re-applies an undo (e.g. a kSystemAllocPage physical free) onto a page a
  // later COMMITTED allocation has already re-issued.
  // |body_restore_anchors| (from RecoverBrokenPages) voids that evidence when
  // a newer body-restoring anchor resurrected pre-CLR content on the page:
  // the undo is re-attempted, and LogUndo's per-record type check keeps a
  // recycled incarnation safe. Allocation undos never take the voided path,
  // so a re-issued live page cannot be re-freed.
  Status UndoLoserChains(
      const std::vector<lsn_t>& loser_heads, UndoneRecorder* undone,
      TransactionManager* tm, lsn_t scan_end, lsn_t scan_start = 0,
      const std::unordered_set<txn_id_t>* known_committed = nullptr,
      const std::map<std::tuple<txn_id_t, int, page_id_t, std::string>,
                     std::pair<lsn_t, lsn_t>>* compensations = nullptr,
      const std::unordered_map<page_id_t, lsn_t>* body_restore_anchors =
          nullptr);

  inline static std::atomic<bool> torn_tail_truncation_allowed_{false};

  std::string log_name_;
  size_t segment_size_;
  // Cached read fds per segment index, guarded by read_fd_mutex_; at most
  // kMaxCachedReadFds stay open (scans run forward, loser chains backward).
  static constexpr size_t kMaxCachedReadFds = 8;
  mutable std::unordered_map<size_t, std::shared_ptr<ReadFd>> segment_fds_;
  // Guards segment_fds_ against concurrent readers.
  mutable std::mutex read_fd_mutex_;
  // Live logger whose ring buffer serves not-yet-written WAL bytes; see
  // SetLogger. Not owned.
  const Logger* logger_{nullptr};
  PagePool* pool_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
