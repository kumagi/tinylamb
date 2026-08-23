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
#include <condition_variable>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <thread>
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
      : lock_manager_(lm), page_manager_(pm), logger_(l), recovery_(r) {
    StartGcWorker();
  }

  // Stops the background GC worker.  Must not race other TransactionManager
  // calls (same teardown contract as the rest of the storage stack).
  ~TransactionManager();

  Transaction Begin(bool read_only = false);

  Status PreCommit(Transaction& txn);

  // When true (default), PreCommit waits until the commit LSN is fsynced.
  // See docs/commit_durability.md.
  void SetSynchronousCommit(bool enabled) {
    synchronous_commit_.store(enabled);
  }
  [[nodiscard]] bool SynchronousCommit() const {
    return synchronous_commit_.load();
  }

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
  // owner defaults to an anonymous holder for tests; production callers pass
  // the transaction id so locks can only be released by their owner.
  bool GetExclusiveLock(const RowPosition& rp, txn_id_t owner = 0);
  bool GetSharedLock(const RowPosition& rp, txn_id_t owner = 0);

  bool TryUpgradeLock(const RowPosition& rp, txn_id_t owner = 0);

  [[nodiscard]] uint64_t CurrentCommitTimestamp() const {
    return commit_timestamp_.load();
  }
  // Highest timestamp whose every version is published and therefore visible
  // to a new snapshot.  Begin() takes snapshots here instead of from
  // commit_timestamp_, whose latest allocation may still be mid-publication.
  [[nodiscard]] uint64_t StableTimestamp() const {
    return stable_timestamp_.load(std::memory_order_acquire);
  }
  StatusOr<std::string> ReadVersion(
      const Transaction& txn, const RowPosition& rp,
      std::optional<std::string_view> physical) const;
  // True when any version chain entry exists for the row.  Without one, the
  // physical row image is the only version and is visible to every snapshot,
  // letting readers skip the copy/cache slow path in Transaction::ReadVersion.
  [[nodiscard]] bool HasVersionChain(const RowPosition& rp) const;
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
  friend class Transaction;

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

  // ---- commit sequencing ----
  // Timestamps are handed out with a plain atomic fetch_add; versions are
  // then published under shard locks alone.  A freshly allocated timestamp is
  // registered as "unpublished" before publication starts, and
  // stable_timestamp_ only ever advances to (smallest unpublished - 1), so a
  // snapshot taken from it can never observe a half-published commit.
  void RegisterPendingCommit(uint64_t ts);
  void PublishCommit(uint64_t ts);

  // ---- background GC ----
  static constexpr uint64_t kGcCommitThreshold = 8;
  void StartGcWorker();
  void GcWorkerLoop();

  // Transaction move operations use these to keep active_transactions_
  // pointing at the live object: Begin() registers the address of the
  // Transaction it returns, and a move must carry the registration over to
  // the new address instead of leaving a pointer to the moved-from object.
  void MoveActiveTransaction(Transaction* from, Transaction* to);
  void UnregisterActiveTransaction(Transaction* txn);

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
  std::atomic<uint64_t> stable_timestamp_{0};
  mutable std::mutex pending_commits_mutex_;
  std::set<uint64_t> unpublished_commits_;
  std::atomic<uint64_t> max_committed_begin_ts_{0};
  std::atomic<int> pending_txn_count_{0};
  mutable std::array<VersionShard, kVersionShardCount> version_shards_;
  std::unordered_map<txn_id_t, uint64_t> active_snapshots_;
  LockManager* const lock_manager_;
  PageManager* const page_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  mutable std::mutex transaction_table_lock;
  std::atomic<bool> synchronous_commit_{true};

  // Background GC (declared last so the worker thread only starts after
  // every member it may touch is fully constructed).
  std::atomic<uint64_t> commits_since_gc_{0};
  std::atomic<bool> gc_stop_{false};
  std::mutex gc_mutex_;
  std::condition_variable gc_cv_;
  std::thread gc_worker_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
