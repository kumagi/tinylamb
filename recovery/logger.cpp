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

#include "recovery/logger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include "common/constants.hpp"
#include "common/log_message.hpp"

namespace tinylamb {

namespace {
StatusOr<int> CreateFile(const std::filesystem::path& path) {
  std::error_code ec;
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
      return StatusError(StatusCode::kIOError,
                         "Failed to create log directory for " + path.string() +
                             ": " + ec.message());
    }
  }
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0666);
  if (fd == -1) {
    return StatusError(
        StatusCode::kIOError,
        "Failed to open log file: " + std::string(std::strerror(errno)) +
            " for " + path.string());
  }
  return fd;
}

// fdatasync (F_FULLFSYNC on macOS) with EINTR retry. The caller must check
// the result: silently skipping a sync would break the durability guarantee.
int FdataSync(int fd) {
  int rc = 0;
#ifdef __APPLE__
  do {
    rc = ::fcntl(fd, F_FULLFSYNC);
  } while (rc < 0 && errno == EINTR);
#else
  do {
    rc = ::fdatasync(fd);
  } while (rc < 0 && errno == EINTR);
#endif
  return rc;
}

}  // namespace

StatusOr<std::unique_ptr<Logger>> Logger::Create(
    const std::filesystem::path& logfile, size_t buffer_size, size_t every_ms) {
  ASSIGN_OR_RETURN(int, fd, CreateFile(logfile));
  std::error_code ec;
  // All state must be consistent before the worker starts; otherwise the
  // worker may observe flushed_lsn_ == 0 and append zeros over an existing
  // WAL.
  const lsn_t file_size = std::filesystem::file_size(logfile, ec);
  if (ec) {
    close(fd);
    return StatusError(
        StatusCode::kIOError,
        "Failed to stat log file " + logfile.string() + ": " + ec.message());
  }
  // std::thread has no noexcept construction path; EAGAIN/ENOMEM from the
  // pthread layer is caught at this boundary and mapped to a Status.
  std::unique_ptr<Logger> logger;
  try {
    logger = std::unique_ptr<Logger>(
        new Logger(logfile, buffer_size, every_ms));  // NOLINT
  } catch (const std::bad_alloc&) {
    // The fd is still owned by this frame; ~Logger has not run, so close it
    // here or the descriptor leaks on an allocation failure.
    close(fd);
    return StatusError(StatusCode::kIOError,
                       "Failed to allocate logger for " + logfile.string());
  }
  // Transfer fd ownership before anything that can throw: the destructor
  // closes dst_, so a Logger constructor failure must not leave the fd
  // stored nowhere, and a spawn failure must not double-close.
  logger->dst_ = fd;
  logger->flushed_lsn_.store(file_size, std::memory_order_relaxed);
  logger->durable_lsn_.store(file_size, std::memory_order_release);
  logger->buffered_lsn_.store(file_size, std::memory_order_release);
  try {
    logger->worker_ = std::thread(&Logger::LoggerWork, logger.get());
  } catch (const std::system_error& error) {
    // ~Logger already closed dst_ via the unique_ptr destruction above.
    return StatusError(StatusCode::kIOError, "Failed to start logger worker: " +
                                                 std::string(error.what()));
  }
  return logger;
}

Logger::Logger(const std::filesystem::path& /*logfile*/, size_t buffer_size,
               size_t every_ms)
    : buffer_(std::max<size_t>(1, buffer_size), 0),
      sync_interval_(std::chrono::milliseconds(std::max<size_t>(1, every_ms))) {
}

Logger::~Logger() {
  DrainAndStopWorker();
  close(dst_);
}

void Logger::NotifyWorker() {
  // Notify under work_mu_: the full-buffer waiters in AddLog evaluate their
  // predicate and block under this mutex, so an unlocked notify can fire
  // between the predicate check and the block and be lost forever (the
  // idle worker never re-notifies work_cv_ on its own).  No caller holds
  // work_mu_ here, and the state stores that precede every call become
  // visible to a later waiter through the mutex.
  std::scoped_lock lock(work_mu_);
  work_cv_.notify_all();
}

void Logger::SetFailed(int err) {
  error_number_.store(err, std::memory_order_relaxed);
  failed_.store(true, std::memory_order_release);
  LOG(ERROR) << dst_ << " : " << std::strerror(err);
  {
    std::scoped_lock lock(durable_mu_);
  }
  durable_cv_.notify_all();
  {
    std::scoped_lock lock(work_mu_);
  }
  work_cv_.notify_all();
}

Status Logger::CheckFailed() const {
  if (!failed_.load(std::memory_order_acquire)) {
    return Status::kSuccess;
  }
  return StatusError(StatusCode::kIOError,
                     "Logger write failed: " +
                         std::string(std::strerror(
                             error_number_.load(std::memory_order_acquire))));
}

void Logger::AdvanceDurable(lsn_t to) {
  {
    std::scoped_lock lock(durable_mu_);
    if (to > durable_lsn_.load(std::memory_order_relaxed)) {
      durable_lsn_.store(to, std::memory_order_release);
    }
  }
  durable_cv_.notify_all();
}

Status Logger::WaitForDurable(lsn_t lsn) {
  pending_durable_waiters_.fetch_add(1, std::memory_order_acq_rel);
  const bool already_satisfied =
      failed_.load(std::memory_order_acquire) ||
      durable_lsn_.load(std::memory_order_acquire) >= lsn;
  if (!already_satisfied) {
    // Nudge the worker: it will fsync promptly when it sees the non-zero
    // waiter count even if the timer has not elapsed.
    NotifyWorker();
    std::unique_lock lock(durable_mu_);
    durable_cv_.wait(lock, [&] {
      return failed_.load(std::memory_order_acquire) ||
             durable_lsn_.load(std::memory_order_acquire) >= lsn;
    });
  }
  pending_durable_waiters_.fetch_sub(1, std::memory_order_acq_rel);
  return CheckFailed();
}

void Logger::TruncateTo(lsn_t valid_end) {
  // Serialize with producers: hold the enqueue latch so no AddLog can be
  // in flight between the file truncation (done by the caller) and the LSN
  // reset.  Recovery calls this before the database serves traffic, so the
  // worker is idle with flushed == durable == buffered == old file size.
  std::unique_lock enq_lk{enqueue_latch_};
  flushed_lsn_.store(valid_end, std::memory_order_relaxed);
  durable_lsn_.store(valid_end, std::memory_order_release);
  buffered_lsn_.store(valid_end, std::memory_order_release);
  durable_cv_.notify_all();
}

void Logger::AdviseOldBytesDurable(lsn_t before) const {
  if (before == 0 || dst_ < 0) {
    return;
  }
#ifdef POSIX_FADV_DONTNEED
  // Drop [0, before) from page cache: those bytes are now safe to discard
  // because the checkpoint that anchored them has been durably committed to
  // the master record. Without this hint fdatasync keeps having to wait for
  // every dirty page from the start of the WAL to reach disk, which on long
  // TPC-C runs turns a sub-millisecond barrier into a multi-millisecond one.
  ::posix_fadvise(dst_, 0, static_cast<off_t>(before), POSIX_FADV_DONTNEED);
#else
  (void)before;
#endif
}

void Logger::DrainAndStopWorker() {
  finish_.store(true, std::memory_order_release);
  NotifyWorker();
  while (!failed_.load(std::memory_order_acquire) &&
         flushed_lsn_.load(std::memory_order_acquire) <
             buffered_lsn_.load(std::memory_order_acquire)) {
    std::unique_lock lock(work_mu_);
    work_cv_.wait_for(lock, std::chrono::milliseconds(10), [&] {
      return failed_.load(std::memory_order_acquire) ||
             flushed_lsn_.load(std::memory_order_acquire) >=
                 buffered_lsn_.load(std::memory_order_acquire);
    });
  }
  if (worker_.joinable()) {
    worker_.join();
  }
}

Status Logger::Finish() {
  DrainAndStopWorker();
  return CheckFailed();
}

StatusOr<lsn_t> Logger::AddLog(std::string_view payload) {
  RETURN_IF_FAIL(CheckFailed());
  // D1: refuse records the recovery reader could never parse back.
  if (payload.size() > kMaxRecordSize) {
    return StatusError(StatusCode::kTooBigData,
                       "WAL record exceeds Logger::kMaxRecordSize: " +
                           std::to_string(payload.size()) + " > " +
                           std::to_string(kMaxRecordSize));
  }
  std::unique_lock enq_lk{enqueue_latch_};
  const lsn_t lsn = buffered_lsn_.load(std::memory_order_relaxed);
  while (!payload.empty()) {
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_seq_cst);
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_seq_cst);

    if (buffered_lsn - flushed_lsn == buffer_.size()) {
      // D1: the enqueue latch is held until the ENTIRE record has landed in
      // the ring buffer, so no other producer can fragment this record's byte
      // stream.  Waiting for flush progress while holding the latch is safe
      // because the flush worker never takes enqueue_latch_ (see
      // docs/lock_order.md); a full buffer drains without our help.
      std::unique_lock work_lk(work_mu_);
      work_cv_.wait(work_lk, [&] {
        return failed_.load(std::memory_order_acquire) ||
               buffered_lsn_.load(std::memory_order_acquire) -
                       flushed_lsn_.load(std::memory_order_acquire) <
                   buffer_.size();
      });
      RETURN_IF_FAIL(CheckFailed());
      continue;
    }
    const size_t buffered = buffered_lsn % buffer_.size();
    const size_t flushed = flushed_lsn % buffer_.size();
    const size_t write_size =
        flushed <= buffered
            ? std::min(payload.size(), buffer_.size() - buffered)
            : std::min(payload.size(), flushed - buffered);
    memcpy(buffer_.data() + buffered, payload.data(), write_size);

    // Forward buffer pointer.
    buffered_lsn_.store(buffered_lsn + write_size, std::memory_order_release);
    payload.remove_prefix(write_size);
  }
  enq_lk.unlock();
  NotifyWorker();
  return lsn;
}

void Logger::LoggerWork() {
  assert(!buffer_.empty());
  using Clock = std::chrono::steady_clock;
  auto last_sync = Clock::now();
  bool dirty = false;
  while (!failed_.load(std::memory_order_acquire) &&
         (!finish_.load(std::memory_order_acquire) ||
          flushed_lsn_.load(std::memory_order_acquire) <
              buffered_lsn_.load(std::memory_order_acquire) ||
          dirty)) {
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_relaxed);
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_acquire);

    if (flushed_lsn == buffered_lsn) {
      if (dirty &&
          (finish_.load(std::memory_order_acquire) ||
           Clock::now() - last_sync >= sync_interval_ ||
           pending_durable_waiters_.load(std::memory_order_acquire) > 0)) {
        if (FdataSync(dst_) != 0) {
          SetFailed(errno);
          return;
        }
        last_sync = Clock::now();
        dirty = false;
        AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
      }
      if (finish_.load(std::memory_order_acquire) &&
          flushed_lsn_.load(std::memory_order_acquire) >=
              buffered_lsn_.load(std::memory_order_acquire) &&
          !dirty) {
        break;
      }
      std::unique_lock work_lk(work_mu_);
      work_cv_.wait_for(work_lk, sync_interval_, [&] {
        return finish_.load(std::memory_order_acquire) ||
               flushed_lsn_.load(std::memory_order_acquire) <
                   buffered_lsn_.load(std::memory_order_acquire) ||
               pending_durable_waiters_.load(std::memory_order_acquire) > 0;
      });
      continue;
    }

    while (flushed_lsn_.load(std::memory_order_relaxed) <
           buffered_lsn_.load(std::memory_order_acquire)) {
      const size_t flushed = flushed_lsn_.load(std::memory_order_relaxed);
      const size_t buffered = buffered_lsn_.load(std::memory_order_acquire);
      const size_t flushed_off = flushed % buffer_.size();
      const size_t buffered_off = buffered % buffer_.size();
      // Retry a signal-interrupted write that moved no bytes; treating
      // EINTR as fatal would fail live transactions spuriously.
      ssize_t flushed_size = 0;
      do {
        flushed_size =
            write(dst_, buffer_.data() + flushed_off,
                  (flushed_off < buffered_off ? buffered_off : buffer_.size()) -
                      flushed_off);
      } while (flushed_size < 0 && errno == EINTR);
      if (flushed_size < 0) {
        // Wake every waiter instead of leaving synchronous commits blocked
        // forever on a dead worker (e.g. ENOSPC / EIO).
        SetFailed(errno);
        return;
      }
      if (flushed_size == 0) {
        // A zero return on a non-zero write() is not a valid outcome and
        // leaves errno meaningless; fail with an explicit code instead of
        // reporting whatever the last syscall left behind.
        SetFailed(EIO);
        return;
      }
      flushed_lsn_.store(flushed + static_cast<size_t>(flushed_size),
                         std::memory_order_release);
      dirty = true;
      NotifyWorker();
    }
    // Adaptive group commit: if any producer is blocked in WaitForDurable,
    // fsync immediately so they wake up on this iteration rather than after
    // the full sync_interval_ of latency. A bare timer would otherwise cap
    // throughput at floor(1 second / sync_interval_) * num_producers_per_fsync
    // barriers per second even when fsync itself is sub-millisecond.
    const bool timer_elapsed = Clock::now() - last_sync >= sync_interval_;
    const bool waiter_present =
        pending_durable_waiters_.load(std::memory_order_acquire) > 0;
    if (finish_.load(std::memory_order_acquire) || timer_elapsed ||
        waiter_present) {
      if (FdataSync(dst_) != 0) {
        SetFailed(errno);
        return;
      }
      last_sync = Clock::now();
      dirty = false;
      AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
    }
  }
  // Final durability barrier before shutdown; a failure here means buffered
  // records were never persisted, so surface it through the failed state.
  if (::fsync(dst_) != 0) {
    SetFailed(errno);
    return;
  }
  AdvanceDurable(flushed_lsn_.load(std::memory_order_relaxed));
  NotifyWorker();
}

}  // namespace tinylamb
