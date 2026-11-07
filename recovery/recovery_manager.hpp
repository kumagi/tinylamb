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
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class PageManager;
class Logger;
class TransactionManager;
struct LogRecord;

// LSM Fuzzy Crash Recovery Manager.
// Restores the latest fuzzy checkpoint from PageManager and replays only
// committed transactions from the WAL segments.
class RecoveryManager {
 public:
  RecoveryManager(RecoveryManager&&) = delete;
  RecoveryManager& operator=(RecoveryManager&&) = delete;
  RecoveryManager(const RecoveryManager&) = delete;
  RecoveryManager& operator=(const RecoveryManager&) = delete;

  static void SetTornTailTruncationAllowed(bool allowed) {
    torn_tail_truncation_allowed_.store(allowed, std::memory_order_relaxed);
  }
  [[nodiscard]] static bool TornTailTruncationAllowed() {
    return torn_tail_truncation_allowed_.load(std::memory_order_relaxed);
  }

  RecoveryManager(std::string_view log_path, PageManager* pm,
                  size_t segment_size = size_t{16} << 20);
  ~RecoveryManager();

  void SetLogger(const Logger* logger) { logger_ = logger; }

  Status RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  bool ReadLog(lsn_t lsn, LogRecord* dst) const;
  [[nodiscard]] uint8_t DebugReadStatusFor(lsn_t lsn) const;

  [[nodiscard]] lsn_t LogStart() const;
  [[nodiscard]] lsn_t LogEnd() const;
  [[nodiscard]] lsn_t ValidLogEnd(lsn_t from) const;

  friend std::ostream& operator<<(std::ostream& o, const RecoveryManager& rm) {
    o << "RecoveryManager(log=" << rm.log_name_ << ")";
    return o;
  }

 private:
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
  [[nodiscard]] std::optional<lsn_t> PreRollPaddingEnd(
      lsn_t lsn, bool* missing_successor = nullptr) const;
  [[nodiscard]] std::shared_ptr<ReadFd> SegmentFdFor(lsn_t lsn) const;
  [[nodiscard]] LogReadResult ReadLogDetailed(lsn_t lsn, LogRecord* dst) const;
  [[nodiscard]] StatusOr<lsn_t> LogStartStatus() const;
  [[nodiscard]] StatusOr<lsn_t> LogEndStatus() const;
  [[nodiscard]] StatusOr<lsn_t> ValidLogEndStatus(lsn_t from) const;

  inline static std::atomic<bool> torn_tail_truncation_allowed_{false};

  std::string log_name_;
  size_t segment_size_;
  bool fixed_size_segments_{false};
  static constexpr size_t kMaxCachedReadFds = 8;
  mutable std::unordered_map<size_t, std::shared_ptr<ReadFd>> segment_fds_;
  mutable std::mutex read_fd_mutex_;
  const Logger* logger_{nullptr};
  PageManager* pm_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
