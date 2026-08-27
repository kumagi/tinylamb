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
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/constants.hpp"

namespace tinylamb {

class Logger final {
 public:
  explicit Logger(const std::filesystem::path& logfile,
                  size_t buffer_size = 1024 * 1024 * 8, size_t every_ms = 1);
  Logger(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger& operator=(Logger&&) = delete;
  ~Logger();

  void Finish();

  // Bytes written to the log file (may not be fsynced yet).
  [[nodiscard]] lsn_t CommittedLSN() const { return flushed_lsn_; }
  // Bytes that have survived fdatasync (durable).
  [[nodiscard]] lsn_t DurableLSN() const {
    return durable_lsn_.load(std::memory_order_acquire);
  }
  [[nodiscard]] lsn_t BufferedLSN() const { return buffered_lsn_; }

  // Appends payload to the write buffer and returns the LSN at which the
  // record STARTS. The record is only enqueued: nothing is flushed yet.
  // To wait for the record's own durability, pass BufferedLSN() read AFTER
  // this call to WaitForDurable() (AddLog()'s return value alone only
  // guarantees the PREVIOUS records are durable).
  lsn_t AddLog(std::string_view payload);

  // Block until DurableLSN() >= lsn (group commit).
  // Throws std::runtime_error if the worker hit an unrecoverable write error.
  void WaitForDurable(lsn_t lsn);

  // Tell the kernel that the bytes [0, before) are no longer needed in page
  // cache. Called by the checkpoint manager after a successful checkpoint so
  // fdatasync no longer has to wait for those pages to be flushed on the next
  // barrier. No-op on platforms without posix_fadvise (e.g. macOS).
  void AdviseOldBytesDurable(lsn_t before);

  [[nodiscard]] int Fd() const { return dst_; }

  // True once the worker thread gave up after a write error (e.g. ENOSPC,
  // EIO). Every subsequent AddLog/WaitForDurable/Finish throws.
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
  void LoggerWork();
  void AdvanceDurable(lsn_t to);
  void NotifyWorker();
  // Marks the logger as failed, wakes every waiter and stops the worker.
  void SetFailed(int err);
  // Throws std::runtime_error when Failed().
  void RaiseIfFailed() const;
  // Signals the worker to quit and joins it. Never throws.
  void DrainAndStopWorker();

  std::atomic<bool> finish_ = false;
  alignas(64) std::atomic<bool> failed_{false};
  alignas(64) std::atomic<int> error_number_{0};
  alignas(64) std::atomic<lsn_t> flushed_lsn_{0};
  alignas(64) std::atomic<lsn_t> durable_lsn_{0};
  // Number of producers currently blocked inside WaitForDurable waiting for
  // the next fsync to complete. The worker checks this counter after each
  // write batch: when the count is non-zero the producers are spinning on
  // the durable barrier, so we fsync right away instead of waiting the full
  // sync_interval_ before waking them. This cuts the tail latency of a
  // producer whose enqueue raced the timer by up to sync_interval_.
  alignas(64) std::atomic<int> pending_durable_waiters_{0};
  std::string buffer_;
  std::chrono::milliseconds sync_interval_{1};
  int dst_ = -1;
  std::mutex enqueue_latch_;
  std::mutex work_mu_;
  std::condition_variable work_cv_;
  std::mutex durable_mu_;
  std::condition_variable durable_cv_;
  alignas(64) std::atomic<lsn_t> buffered_lsn_{0};
  std::thread worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOGGER_HPP
