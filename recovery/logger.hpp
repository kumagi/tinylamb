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

#ifndef TINYLAMB_LOGGER_HPP
#define TINYLAMB_LOGGER_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

// Fields are grouped by locking domain (enqueue/work/durable), not by size.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
class Logger final {
 public:
  // D1: a WAL record must stay within the bound the recovery reader accepts
  // (RecoveryManager parses records from a 16 MiB window, and the serialized
  // record carries magic/version/header/CRC overhead on top of the payload).
  // AddLog rejects anything larger instead of writing bytes that could never
  // be replayed.
  static constexpr size_t kMaxRecordSize =
      (size_t{16} << 20) - (size_t{64} << 10);

  // The WAL is cut into fixed-size segment files named
  // `<logfile>.<20-digit index>`; a record's absolute LSN maps to
  // `index = lsn / kLogSegmentSize`, `offset = lsn % kLogSegmentSize`.
  // Segmenting is what makes recycling possible: after a durable checkpoint,
  // CheckpointManager deletes whole segments whose records can no longer be
  // needed (see docs/wal_format.md), so a long-running database stops
  // growing its log without renumbering LSNs or moving live bytes.
  static constexpr size_t kDefaultLogSegmentSize = size_t{16} << 20;

  // LSN address -> segment file name. Zero padding keeps `ls` output sorted
  // and makes the suffix unambiguous against other `<name>.log*` artifacts.
  static std::filesystem::path SegmentPath(const std::filesystem::path& base,
                                           uint64_t index);
  static std::filesystem::path SegmentMetadataPath(
      const std::filesystem::path& base);
  static constexpr size_t SegmentIndex(lsn_t lsn, size_t segment_size) {
    return static_cast<size_t>(lsn / segment_size);
  }
  // Index of the highest `base.<index>` file in the directory, and whether
  // any segment exists at all. Shared with RecoveryManager, which resolves
  // absolute LSNs to (segment, offset) when replaying.
  static StatusOr<uint64_t> FindHighestSegment(
      const std::filesystem::path& base, bool* any);
  static StatusOr<uint64_t> FindLowestSegment(const std::filesystem::path& base,
                                              bool* any);

  // Opens (creating when needed) the WAL segment series rooted at `logfile`
  // and starts the flush worker. A pre-segmentation single-file log is
  // migrated into segments first. Failure to open a file, stat it, or spawn
  // the worker is reported as a Status instead of an exception; no partially
  // started logger leaks.
  static StatusOr<std::unique_ptr<Logger>> Create(
      const std::filesystem::path& logfile,
      size_t buffer_size = size_t{1024} * 1024 * 8, size_t every_ms = 1,
      size_t segment_size = kDefaultLogSegmentSize);
  Logger(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger& operator=(Logger&&) = delete;
  ~Logger();

  // Drains the worker and reports a write failure, if one occurred.
  Status Finish();

  // Bytes written to the log files (may not be fsynced yet).
  [[nodiscard]] lsn_t CommittedLSN() const { return flushed_lsn_; }
  // Bytes that have survived fdatasync (durable).
  [[nodiscard]] lsn_t DurableLSN() const {
    return durable_lsn_.load(std::memory_order_acquire);
  }
  [[nodiscard]] lsn_t BufferedLSN() const { return buffered_lsn_; }
  // Lowest LSN the ring could ever serve (the open-time WAL end); bytes
  // below it are only readable from the segment files. Diagnostic use.
  [[nodiscard]] lsn_t RingFloorLSN() const {
    return ring_floor_lsn_.load(std::memory_order_acquire);
  }
  [[nodiscard]] size_t SegmentSize() const { return segment_size_; }

  // Appends payload to the write buffer and returns the LSN at which the
  // record STARTS. The record is only enqueued: nothing is flushed yet.
  // To wait for the record's own durability, pass BufferedLSN() read AFTER
  // this call to WaitForDurable() (AddLog()'s return value alone only
  // guarantees the PREVIOUS records are durable).
  // Returns a failure Status when the worker has died or the payload exceeds
  // kMaxRecordSize.
  StatusOr<lsn_t> AddLog(std::string_view payload);

  // Block until DurableLSN() >= lsn (group commit).
  // Returns a failure Status if the worker hit an unrecoverable write error.
  Status WaitForDurable(lsn_t lsn);

  // Resynchronize after recovery truncated the log to `valid_end`
  // (torn-tail --force path). Truncates the tail segment to `valid_end`,
  // deletes every segment above it, and reopens the append fd there. The
  // constructor latched the three LSNs to the pre-truncation file size;
  // without this reset every later AddLog reports an LSN past the real
  // record position, desynchronizing page stamps and chain walks from the
  // files. Must be called before the database serves any traffic.
  Status TruncateTo(lsn_t valid_end);

  // Copies up to |len| bytes of the virtual WAL stream starting at |lsn|
  // from the in-memory ring buffer into |dst|, and returns true when the
  // WHOLE range was served. False means at least one byte is not resident
  // (below the ring floor, already overwritten by the ring wrap, or above
  // the buffered end) and the caller must fall back to the segment files.
  //
  // This is what lets a runtime undo walk read its own records back while
  // they are still volatile: TransactionManager::Abort used to pay a full
  // group-commit flush (of every transaction's buffered records, not just
  // its own) before the walk purely so ReadLog could see them on disk.
  // Durability of abort records is not required for correctness -- a crash
  // before they reach the files replays the same rollback as restart loser
  // cleanup -- so serving the bytes from the ring removes that wait.
  //
  // Lock-free and safe against concurrent appends: producers only overwrite
  // ring position x after the buffered pointer passes x + buffer size, and
  // the copied range is re-verified against a fresh buffered load after the
  // memcpy (the pointer is monotonic, so a passing re-check proves no byte
  // of the range was rewritten while it was being copied). Bytes below the
  // open-time WAL end (ring_floor_lsn_) never passed through this ring --
  // the buffer starts zeroed -- and are never served.
  bool TryCopyBuffered(lsn_t lsn, char* dst, size_t len) const;

  // Tell the kernel that bytes below `before` are no longer needed in page
  // cache. Called by the checkpoint manager after a successful checkpoint so
  // fdatasync no longer has to wait for those pages to be flushed on the next
  // barrier. No-op on platforms without posix_fadvise (e.g. macOS).
  void AdviseOldBytesDurable(lsn_t before) const;

  // Deletes every whole segment whose records all sit below `floor`
  // (segment (i) is removable when (i+1) * segment_size <= floor). The
  // current segment is never removed. Before removing segment (i) the
  // recycle boundary validator (SetRecycleBoundaryValidator) must confirm
  // that byte (i+1) * segment_size starts a record: a legacy record may
  // straddle a segment boundary, and deleting the segment holding its head
  // would orphan its tail. Call only once the checkpoint anchored at >=
  // floor is durably recorded in the master record AND every page image the
  // recycled records could rebuild is durably flushed (see
  // CheckpointManager::WriteCheckpoint). Returns the new lowest retained
  // byte (the end of the highest removed segment), or 0 when nothing was
  // removed. A segment that cannot be unlinked is logged and left in place.
  lsn_t RecycleUpTo(lsn_t floor);

  // Installs the boundary validator used by RecycleUpTo: validator(lsn)
  // returns true when `lsn` is the first byte of a decodable record. Wired
  // by the database layer with RecoveryManager::ReadLog; without it, every
  // boundary is assumed to be a record start (raw logger usage only).
  void SetRecycleBoundaryValidator(std::function<bool(lsn_t)> validator);

  // Lowest LSN that still has a segment file on disk (the retained prefix
  // starts here after recycling). 0 when segment 0 exists.
  [[nodiscard]] lsn_t LowestRetainedLSN() const;

  [[nodiscard]] int Fd() const { return dst_; }

  // True once the worker thread gave up after a write error (e.g. ENOSPC,
  // EIO). Every subsequent AddLog/WaitForDurable/Finish returns a failure.
  [[nodiscard]] bool Failed() const {
    return failed_.load(std::memory_order_acquire);
  }
  // errno captured at the failing write; valid only when Failed().
  [[nodiscard]] int ErrorNumber() const {
    return error_number_.load(std::memory_order_acquire);
  }

  friend std::ostream& operator<<(std::ostream& o, const Logger& l) {
    o << "Logger(committed_lsn=" << l.flushed_lsn_.load()
      << ", durable_lsn=" << l.durable_lsn_.load()
      << ", buffered_lsn=" << l.buffered_lsn_.load()
      << ", finish=" << l.finish_.load() << ")";
    return o;
  }

 private:
  // Only Create() may construct; use it so open/stat/thread-start failures
  // surface as Status.
  explicit Logger(std::filesystem::path logfile, size_t buffer_size,
                  size_t every_ms, size_t segment_size);

  // Copies a pre-segmentation single-file WAL into segment files. Idempotent:
  // a no-op when segments already exist. Runs before the worker starts.
  static Status MigrateLegacyLog(const std::filesystem::path& base,
                                 size_t segment_size);
  static StatusOr<size_t> ReadSegmentMetadata(const std::filesystem::path& base,
                                              bool* exists);
  static Status WriteSegmentMetadata(const std::filesystem::path& base,
                                     size_t segment_size);

  void LoggerWork();
  // Closes dst_ and opens the append fd for `index` (O_CREAT | O_APPEND).
  Status RollToLocked(uint64_t index);
  void AdvanceDurable(lsn_t to);
  void NotifyWorker();
  // Marks the logger as failed, wakes every waiter and stops the worker.
  void SetFailed(int err);
  // Returns a failure Status when Failed(), success otherwise.
  [[nodiscard]] Status CheckFailed() const;
  // Signals the worker to quit and joins it. Never throws.
  void DrainAndStopWorker();

  std::filesystem::path base_path_;
  size_t segment_size_ = kDefaultLogSegmentSize;
  mutable std::mutex segment_mu_;
  // Index of the segment dst_ currently appends to.
  uint64_t current_segment_ = 0;
  // Lowest segment index that may still exist on disk. RecycleUpTo removes
  // the files and advances this under segment_mu_, so RecycleUpTo /
  // LowestRetainedLSN never re-scan the (ever growing) removed prefix: a
  // database that recycles a segment per checkpoint would otherwise pay an
  // O(total-history) directory walk on every checkpoint, holding the mutex
  // the WAL writer needs for its segment lookups.
  uint64_t lowest_segment_ = 0;

  std::atomic<bool> finish_ = false;
  alignas(64) std::atomic<bool> failed_{false};
  alignas(64) std::atomic<int> error_number_{0};
  alignas(64) std::atomic<lsn_t> flushed_lsn_{0};
  alignas(64) std::atomic<lsn_t> durable_lsn_{0};
  // Sleep-state hint for the group-commit wakeup protocol. The worker stores
  // true (under work_mu_) immediately before blocking on work_cv_ and false
  // after waking. A producer that observes false skips its NotifyWorker: the
  // worker is awake and re-reads buffered_lsn_ before it can block again, so
  // the freshly enqueued bytes are picked up on that pass. A stale false can
  // delay an ASYNC record by at most one sync_interval_ (the idle wait is a
  // timed wait); synchronous commits always wake the worker through
  // WaitForDurable, so durability latency is unaffected. This removes one
  // futex wake per appended record from the hot path.
  alignas(64) std::atomic<bool> worker_sleeping_{false};
  // Number of producers currently blocked inside WaitForDurable waiting for
  // the next fsync to complete. The worker checks this counter after each
  // write batch: when the count is non-zero the producers are spinning on
  // the durable barrier, so we fsync right away instead of waiting the full
  // sync_interval_ before waking them. This cuts the tail latency of a
  // producer whose enqueue raced the timer by up to sync_interval_.
  alignas(64) std::atomic<int> pending_durable_waiters_{0};
  // Producers currently parked in AddLog's full-buffer wait. Guarded by
  // work_mu_ (both the park and the worker's read take it), so the worker can
  // distinguish "nobody waiting" from "someone needs the futex wake" without
  // sampling ring occupancy -- occupancy snapshots race the producer's
  // park/progress and once lost the wakeup for an unbounded wait.
  int full_buffer_waiters_{0};
  std::string buffer_;
  std::chrono::milliseconds sync_interval_{1};
  int dst_ = -1;
  // See SetRecycleBoundaryValidator. Read without extra locking; installed
  // before the checkpoint worker starts recycling.
  std::function<bool(lsn_t)> recycle_boundary_validator_;
  std::mutex enqueue_latch_;
  std::mutex work_mu_;
  std::condition_variable work_cv_;
  std::mutex durable_mu_;
  std::condition_variable durable_cv_;
  alignas(64) std::atomic<lsn_t> buffered_lsn_{0};
  // Lowest LSN whose bytes ever passed through the ring buffer in this
  // process lifetime: seeded to the open-time WAL end (a reopened database's
  // ring starts zeroed -- the file bytes below this LSN are not in it) and
  // advanced on TruncateTo. TryCopyBuffered never serves below it.
  alignas(64) std::atomic<lsn_t> ring_floor_lsn_{0};
  std::thread worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
