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
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {

class Page;
class PageManager;
class PagePool;
class PageRef;
class Transaction;
class TransactionManager;
struct LogRecord;

class RecoveryManager {
 public:
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
      return lsns_.find(lsn) != lsns_.end();
    }

   private:
    std::mutex mu_;
    std::unordered_set<lsn_t> lsns_;
  };

  // Corruption policy: by default a torn/unparsable WAL tail is treated as
  // unrecoverable damage -- boot logs the offset and aborts. When enabled
  // (e.g. the `--force` CLI flag), recovery truncates the damaged tail and
  // replays the intact prefix instead.
  static void SetTornTailTruncationAllowed(bool allowed) {
    torn_tail_truncation_allowed_.store(allowed, std::memory_order_relaxed);
  }
  [[nodiscard]] static bool TornTailTruncationAllowed() {
    return torn_tail_truncation_allowed_.load(std::memory_order_relaxed);
  }

  RecoveryManager(std::string_view log_path, PagePool* pp);
  ~RecoveryManager();
  RecoveryManager(const RecoveryManager&) = delete;
  RecoveryManager& operator=(const RecoveryManager&) = delete;

  void SinglePageRecovery(PageRef&& page, TransactionManager* tm,
                          UndoneRecorder* undone = nullptr);

  void RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  bool ReadLog(lsn_t lsn, LogRecord* dst) const;

  // Offset of the first unparseable record at or after `from` (the single
  // torn-tail authority; see D9 in docs/design.md for the CRC extension).
  [[nodiscard]] lsn_t ValidLogEnd(lsn_t from) const;

  void LogUndoWithPage(lsn_t lsn, const LogRecord& log, TransactionManager* tm);

  friend std::ostream& operator<<(std::ostream& o, const RecoveryManager& rm) {
    o << "RecoveryManager(log=" << rm.log_name_ << ")";
    return o;
  }

 private:
  [[nodiscard]] bool OpenReadFd() const;
  void SinglePageRecovery(PageRef&& page, TransactionManager* tm,
                          UndoneRecorder* undone, std::uintmax_t scan_end);
  // Walks each loser transaction's prev_lsn chain newest-first and applies
  // undo to every record not already compensated by the per-page replay.
  void UndoLoserChains(const std::vector<lsn_t>& loser_heads,
                       UndoneRecorder* undone, TransactionManager* tm,
                       lsn_t scan_end);

  inline static std::atomic<bool> torn_tail_truncation_allowed_{false};

  std::string log_name_;
  mutable int read_fd_ = -1;
  // Guards the lazy open of read_fd_ against concurrent readers.
  mutable std::mutex read_fd_mutex_;
  PagePool* pool_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
