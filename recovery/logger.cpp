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
#include <stdexcept>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "common/log_message.hpp"

namespace tinylamb {

namespace {
int CreateFile(const std::filesystem::path& path) {
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path());
  }
  return ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0666);
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

Logger::Logger(const std::filesystem::path& logfile, size_t buffer_size,
               size_t every_ms)
    : buffer_(buffer_size, 0),
      sync_interval_(std::chrono::milliseconds(std::max<size_t>(1, every_ms))),
      dst_(CreateFile(logfile)) {
  if (dst_ == -1) {
    throw std::runtime_error(
        "Failed to open log file: " + std::string(std::strerror(errno)) +
        " for " + logfile.string());
  }
  // All state must be consistent before the worker starts; otherwise the
  // worker may observe flushed_lsn_ == 0 and append zeros over an existing
  // WAL.
  const lsn_t file_size = std::filesystem::file_size(logfile);
  flushed_lsn_.store(file_size, std::memory_order_relaxed);
  durable_lsn_.store(file_size, std::memory_order_release);
  buffered_lsn_.store(file_size, std::memory_order_release);
  worker_ = std::thread(&Logger::LoggerWork, this);
}

Logger::~Logger() {
  DrainAndStopWorker();
  close(dst_);
}

void Logger::NotifyWorker() { work_cv_.notify_all(); }

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

void Logger::RaiseIfFailed() const {
  if (!failed_.load(std::memory_order_acquire)) {
    return;
  }
  throw std::runtime_error("Logger write failed: " +
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

void Logger::WaitForDurable(lsn_t lsn) {
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
  RaiseIfFailed();
}

void Logger::AdviseOldBytesDurable(lsn_t before) {
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

void Logger::Finish() {
  DrainAndStopWorker();
  RaiseIfFailed();
}

lsn_t Logger::AddLog(std::string_view payload) {
  RaiseIfFailed();
  // D1: refuse records the recovery reader could never parse back.
  if (payload.size() > kMaxRecordSize) {
    throw std::invalid_argument("WAL record exceeds Logger::kMaxRecordSize: " +
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
      RaiseIfFailed();
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
      flushed_lsn_.store(flushed + flushed_size, std::memory_order_release);
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
