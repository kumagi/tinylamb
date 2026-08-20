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

#ifndef TINYLAMB_TRANSACTION_MANAGER_HPP
#define TINYLAMB_TRANSACTION_MANAGER_HPP

#include <array>
#include <atomic>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

class IndexKey;
class LockManager;
class Logger;
class PageManager;
class Transaction;
class RecoveryManager;
enum class TransactionStatus : uint_fast8_t;
struct FosterPair;
struct LogRecord;
struct RowPosition;

class TransactionManager {
 public:
  TransactionManager(LockManager* lm, PageManager* pm, Logger* l,
                     RecoveryManager* r)
      : lock_manager_(lm), page_manager_(pm), logger_(l), recovery_(r) {}

  Transaction Begin(bool read_only = false);

  Status PreCommit(Transaction& txn);

  void Abort(Transaction& txn);

  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid, slot_t slot);
  void CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                           std::string_view key);
  void CompensateInsertBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateUpdateLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateUpdateBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key, page_id_t redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, slot_t slot,
                           std::string_view redo);
  void CompensateDeleteLog(txn_id_t txn_id, page_id_t pid, std::string_view key,
                           std::string_view redo);
  void CompensateDeleteBranchLog(txn_id_t txn_id, page_id_t pid,
                                 std::string_view key, page_id_t redo);
  void CompensateSetLowestValueLog(txn_id_t txn_id, page_id_t pid,
                                   page_id_t redo);
  void CompensateSetLowFenceLog(txn_id_t txn_id, page_id_t pid,
                                const IndexKey& redo);
  void CompensateSetHighFenceLog(txn_id_t txn_id, page_id_t pid,
                                 const IndexKey& redo);
  void CompensateSetFosterLog(txn_id_t txn_id, page_id_t pid,
                              const FosterPair& foster);
  bool GetExclusiveLock(const RowPosition& rp);
  bool GetSharedLock(const RowPosition& rp);

  bool TryUpgradeLock(const RowPosition& rp);

  [[nodiscard]] uint64_t CurrentCommitTimestamp() const {
    return commit_timestamp_.load();
  }
  StatusOr<std::string> ReadVersion(
      const Transaction& txn, const RowPosition& rp,
      std::optional<std::string_view> physical) const;
  void RegisterVersionWrite(Transaction& txn, const RowPosition& rp,
                            std::optional<std::string_view> before,
                            std::optional<std::string_view> after);
  [[nodiscard]] bool RequiresHistoricalRead(const Transaction& txn) const;
  [[nodiscard]] bool IndexKeysMayBeStale(const Transaction& txn) const;

  lsn_t AddLog(const LogRecord& lr);
  lsn_t CommittedLSN() const;

  PageManager* GetPageManager() { return page_manager_; }

  friend std::ostream& operator<<(std::ostream& o,
                                  const TransactionManager& tm) {
    std::scoped_lock lk(tm.transaction_table_lock);
    o << "TransactionManager(next_txn_id=" << tm.next_txn_id_.load()
      << ", active=" << tm.active_transactions_.size() << ")";
    return o;
  }

 private:
  friend class RecoveryManager;
  friend class CheckpointManager;

  std::unordered_map<txn_id_t, Transaction*> active_transactions_;
  std::atomic<txn_id_t> next_txn_id_ = 1;
  struct CommittedVersion {
    uint64_t begin_ts{0};
    uint64_t end_ts{std::numeric_limits<uint64_t>::max()};
    std::optional<std::string> value;
  };
  struct PendingVersion {
    txn_id_t owner{0};
    std::optional<std::string> value;
  };
  struct VersionChain {
    std::vector<CommittedVersion> committed;
    std::optional<PendingVersion> pending;
  };
  void CommitVersions(Transaction& txn);
  void AbortVersions(Transaction& txn);
  void ReleaseLocksAndForget(Transaction& txn);
  void GarbageCollectVersions();

  static constexpr size_t kVersionShardCount = 64;

  struct VersionShard {
    mutable std::mutex mutex;
    std::unordered_map<RowPosition, VersionChain> versions;
  };

  [[nodiscard]] static size_t VersionShardIndex(const RowPosition& rp) {
    return static_cast<size_t>((rp.page_id * 131ull) + rp.slot) %
           kVersionShardCount;
  }

  std::atomic<uint64_t> commit_timestamp_{0};
  std::atomic<uint64_t> max_committed_begin_ts_{0};
  std::atomic<int> pending_txn_count_{0};
  mutable std::array<VersionShard, kVersionShardCount> version_shards_;
  std::unordered_map<txn_id_t, uint64_t> active_snapshots_;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  mutable std::mutex transaction_table_lock;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
