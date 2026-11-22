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

  // Process-wide default applied to newly constructed managers (set by the
  // --force CLI flag before any manager exists). Individual instances may
  // override it without affecting other databases in the same process.
  static void SetTornTailTruncationAllowed(bool allowed) {
    default_torn_tail_truncation_allowed_.store(allowed,
                                                std::memory_order_relaxed);
  }
  [[nodiscard]] static bool TornTailTruncationAllowed() {
    return default_torn_tail_truncation_allowed_.load(
        std::memory_order_relaxed);
  }
  void SetTornTailTruncationAllowedForInstance(bool allowed) {
    torn_tail_truncation_allowed_ = allowed;
  }
  [[nodiscard]] bool TornTailTruncationAllowedForInstance() const {
    return torn_tail_truncation_allowed_;
  }

  RecoveryManager(std::string_view log_path, PageManager* pm,
                  size_t segment_size = size_t{16} << 20);
  ~RecoveryManager();

  void SetLogger(Logger* logger) { logger_ = logger; }

  Status RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  enum class WalScanStep : uint8_t {
    kRecord,
    kPaddingSkip,
    kUnavailable,
    // T4225: framing/CRC-destroyed record (vs merely "not readable yet"):
    // lets the reread walker name corruption instead of flattening both.
    kCorrupt,
  };

  bool ReadLog(lsn_t lsn, LogRecord* dst) const;
  [[nodiscard]] WalScanStep ReadWalRecordForward(lsn_t lsn, LogRecord* dst,
                                                 lsn_t* next_lsn_out) const;
  [[nodiscard]] uint8_t DebugReadStatusFor(lsn_t lsn) const;

  [[nodiscard]] lsn_t LogStart() const;
  // An unreadable WAL is distinct from an empty WAL.
  [[nodiscard]] StatusOr<lsn_t> LogEnd() const;
  [[nodiscard]] StatusOr<lsn_t> LogEndStatus() const;
  [[nodiscard]] lsn_t ValidLogEnd(lsn_t from) const;
  // Set by RecoverFrom: the LSN from which WAL was replayed, and whether a
  // torn tail had to be truncated. Used to skip a no-op end-of-recovery
  // checkpoint when the persisted snapshot already covers the whole WAL.
  [[nodiscard]] lsn_t RecoveryAnchorLSN() const noexcept {
    return recovery_anchor_lsn_;
  }
  [[nodiscard]] bool RecoveryTornTailTruncated() const noexcept {
    return recovery_torn_tail_truncated_;
  }

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
  [[nodiscard]] StatusOr<lsn_t> ValidLogEndStatus(lsn_t from) const;

  inline static std::atomic<bool> default_torn_tail_truncation_allowed_{false};
  bool torn_tail_truncation_allowed_{false};

  std::string log_name_;
  size_t segment_size_;
  bool fixed_size_segments_{false};
  // The constructor cannot return a Status; a failed segment-format lookup
  // must be surfaced by RecoverFrom rather than fail-open as "variable size".
  Status format_status_{Status::kSuccess};
  static constexpr size_t kMaxCachedReadFds = 8;
  mutable std::unordered_map<size_t, std::shared_ptr<ReadFd>> segment_fds_;
  mutable std::mutex read_fd_mutex_;
  Logger* logger_{nullptr};
  PageManager* pm_{nullptr};
  lsn_t recovery_anchor_lsn_{0};
  bool recovery_torn_tail_truncated_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
