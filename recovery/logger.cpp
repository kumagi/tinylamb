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
    throw std::runtime_error("Failed to open log file: " +
                             std::string(std::strerror(errno)) + " for " +
                             logfile.string());
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
  std::unique_lock lock(durable_mu_);
  durable_cv_.wait(lock, [&] {
    return failed_.load(std::memory_order_acquire) ||
           durable_lsn_.load(std::memory_order_acquire) >= lsn;
  });
  RaiseIfFailed();
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
  std::unique_lock enq_lk{enqueue_latch_};
  lsn_t lsn = buffered_lsn_.load(std::memory_order_relaxed);
  size_t written = 0;
  while (!payload.empty()) {
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_seq_cst);
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_seq_cst);

    if (buffered_lsn - flushed_lsn == buffer_.size()) {
      // Do not block on the enqueue latch while waiting for flush progress.
      enq_lk.unlock();
      std::unique_lock work_lk(work_mu_);
      work_cv_.wait(work_lk, [&] {
        return failed_.load(std::memory_order_acquire) ||
               buffered_lsn_.load(std::memory_order_acquire) -
                       flushed_lsn_.load(std::memory_order_acquire) <
                   buffer_.size();
      });
      enq_lk.lock();
      RaiseIfFailed();
      if (written == 0) {
        // Nothing of this payload landed yet, so another producer may have
        // consumed LSN space while the enqueue latch was released; a stale
        // value here would corrupt prev_lsn chains.
        lsn = buffered_lsn_.load(std::memory_order_relaxed);
      }
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
    written += write_size;

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
      if (dirty && (finish_.load(std::memory_order_acquire) ||
                    Clock::now() - last_sync >= sync_interval_)) {
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
                   buffered_lsn_.load(std::memory_order_acquire);
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
      if (flushed_size <= 0) {
        // Wake every waiter instead of leaving synchronous commits blocked
        // forever on a dead worker (e.g. ENOSPC / EIO).
        SetFailed(errno);
        return;
      }
      flushed_lsn_.store(flushed + flushed_size, std::memory_order_release);
      dirty = true;
      NotifyWorker();
    }
    if (finish_.load(std::memory_order_acquire) ||
        Clock::now() - last_sync >= sync_interval_) {
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
