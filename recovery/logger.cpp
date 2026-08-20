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
  return ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0666);
}

int FdataSync(int fd) {
#if defined(__APPLE__)
  return ::fcntl(fd, F_FULLFSYNC);
#else
  return fdatasync(fd);
#endif
}

}  // namespace

Logger::Logger(const std::filesystem::path& logfile, size_t buffer_size,
               size_t every_ms)
    : every_us_(every_ms),
      buffer_(buffer_size, 0),
      dst_(CreateFile(logfile)),
      worker_(&Logger::LoggerWork, this) {
  if (dst_ == -1) {
    LOG(FATAL) << "Failed to open log file: " << std::strerror(errno) << " for "
               << logfile;
  }
  flushed_lsn_ = buffered_lsn_ = std::filesystem::file_size(logfile);
  memset(buffer_.data(), 0, buffer_size);
}

Logger::~Logger() {
  Finish();
  close(dst_);
}

void Logger::Finish() {
  finish_ = true;
  while (flushed_lsn_ != buffered_lsn_) {
    // Wait to flush.
  }
  worker_.join();
}

lsn_t Logger::AddLog(std::string_view payload) {
  std::unique_lock enq_lk{enqueue_latch_};
  const lsn_t lsn = buffered_lsn_.load(std::memory_order_relaxed);
  while (!payload.empty()) {
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_seq_cst);
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_seq_cst);

    if (buffered_lsn - flushed_lsn == buffer_.size()) {
      // Do not sleep while holding the enqueue latch: concurrent writers would
      // stall behind a parked holder even after the flusher frees space.
      enq_lk.unlock();
      std::this_thread::sleep_for(std::chrono::microseconds(every_us_ / 2));
      enq_lk.lock();
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
  return lsn;
}

void Logger::LoggerWork() {
  assert(!buffer_.empty());
  using Clock = std::chrono::steady_clock;
  auto last_sync = Clock::now();
  constexpr auto kSyncInterval = std::chrono::milliseconds(10);
  bool dirty = false;
  while (!finish_ || flushed_lsn_ < buffered_lsn_ || dirty) {
    const size_t flushed_lsn = flushed_lsn_.load(std::memory_order_relaxed);
    const size_t buffered_lsn = buffered_lsn_.load(std::memory_order_acquire);

    if (flushed_lsn == buffered_lsn) {
      if (dirty && (finish_ || Clock::now() - last_sync >= kSyncInterval)) {
        FdataSync(dst_);
        last_sync = Clock::now();
        dirty = false;
      }
      if (finish_ && flushed_lsn_ >= buffered_lsn_ && !dirty) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(every_us_));
      continue;
    }

    while (flushed_lsn_.load(std::memory_order_relaxed) <
           buffered_lsn_.load(std::memory_order_acquire)) {
      const size_t flushed = flushed_lsn_.load(std::memory_order_relaxed);
      const size_t buffered = buffered_lsn_.load(std::memory_order_acquire);
      const size_t flushed_off = flushed % buffer_.size();
      const size_t buffered_off = buffered % buffer_.size();
      const ssize_t flushed_size =
          write(dst_, buffer_.data() + flushed_off,
                (flushed_off < buffered_off ? buffered_off : buffer_.size()) -
                    flushed_off);
      if (flushed_size <= 0) {
        LOG(FATAL) << dst_ << " : " << std::strerror(errno);
        return;
      }
      flushed_lsn_.store(flushed + flushed_size, std::memory_order_release);
      dirty = true;
    }
    if (finish_ || Clock::now() - last_sync >= kSyncInterval) {
      FdataSync(dst_);
      last_sync = Clock::now();
      dirty = false;
    }
  }
  fsync(dst_);
}

}  // namespace tinylamb
