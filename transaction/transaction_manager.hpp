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

struct TransactionRuntimeStats {
  uint64_t wal_wait_count{0};
  uint64_t wal_wait_ns{0};
  uint64_t write_intent_attempts{0};
  uint64_t write_intent_conflicts{0};
  uint64_t write_intent_mutex_wait_ns{0};
  uint64_t commit_shard_mutex_wait_ns{0};
};

class IndexKey;
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
  TransactionManager(PageManager* pm, Logger* l, RecoveryManager* r)
      : page_manager_(pm), logger_(l), recovery_(r) {
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
  void SetMetricsEnabled(bool enabled) {
    metrics_enabled_.store(enabled, std::memory_order_relaxed);
  }
  // Deadlock prevention/detection policy for write-intent acquisition.
  //   kWaitDie       -- an older transaction may wait for a younger holder;
  //                     a younger transaction aborts itself instead of waiting.
  //                     This is conservative (no victim is rolled back while
  //                     making progress), so the wait queue is FIFO by id.
  //   kWoundWait     -- a younger transaction is preempted: the older
  //                     arriving transaction aborts its younger holder and
  //                     takes the lock. Starves younger long transactions
  //                     when many older transactions pile on, which fits
  //                     short TPC-C critical sections.
  //   kDeadlockDetect -- permit unrestricted waiting; a background thread
  //                     walks a global wait-for graph and aborts the youngest
  //                     participant in any cycle. Best when cycles are rare
  //                     and lock-hold time is hard to bound.
  //   kLegacy        -- the pre-policy behavior: wait up to a short timeout,
  //                     then abort self. No deadlock prevention, no detection.
  enum class DeadlockPolicy : uint8_t {
    kLegacy = 0,
    kWaitDie = 1,
    kWoundWait = 2,
    kDeadlockDetect = 3,
  };
  void SetDeadlockPolicy(DeadlockPolicy policy) {
    deadlock_policy_.store(static_cast<uint8_t>(policy),
                            std::memory_order_release);
    if (policy == DeadlockPolicy::kDeadlockDetect &&
        !deadlock_detector_.joinable()) {
      deadlock_detector_ = std::thread([this] { DeadlockDetectorLoop(); });
    }
  }
  [[nodiscard]] DeadlockPolicy GetDeadlockPolicy() const {
    return static_cast<DeadlockPolicy>(
        deadlock_policy_.load(std::memory_order_acquire));
  }
  [[nodiscard]] TransactionRuntimeStats RuntimeStats() const;

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
  // Non-waiting first-updater-wins reservation stored in the same shard as
  // the row's MVCC chain. A stale snapshot or another pending writer loses.
  bool AcquireWriteIntent(Transaction& txn, const RowPosition& rp,
                          bool wait);

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
  [[nodiscard]] bool IndexKeysMayBeStale(const Transaction& txn,
                                         page_id_t index_root) const;

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
    bool staged{false};
  };
  struct VersionChain {
    std::vector<CommittedVersion> committed;
    std::optional<PendingVersion> pending;
  };
  void CommitVersions(Transaction& txn);
  void AbortVersions(Transaction& txn);
  void ForgetTransaction(Transaction& txn);
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

  static constexpr size_t kVersionShardCount = 256;
  static constexpr size_t kIndexMutationShardCount = 256;

  struct VersionShard {
    mutable std::mutex mutex;
    std::condition_variable write_intent_released;
    std::unordered_map<RowPosition, VersionChain> versions;
  };

  [[nodiscard]] static size_t VersionShardIndex(const RowPosition& rp) {
    return static_cast<size_t>((rp.page_id * 131ull) + rp.slot) %
           kVersionShardCount;
  }
  [[nodiscard]] static size_t IndexMutationShardIndex(page_id_t root) {
    return static_cast<size_t>(root * 11400714819323198485ull) %
           kIndexMutationShardCount;
  }

  std::atomic<uint64_t> commit_timestamp_{0};
  std::atomic<uint64_t> stable_timestamp_{0};
  mutable std::mutex pending_commits_mutex_;
  std::set<uint64_t> unpublished_commits_;
  std::atomic<uint64_t> max_committed_begin_ts_{0};
  // Conservative per-index mutation epochs. Hash collisions only cause an
  // unnecessary fallback; they can never expose a stale key. This avoids a
  // mutex on every OLTP index probe while preventing an unrelated table's
  // delete from degrading all indexes to full scans.
  std::array<std::atomic<uint64_t>, kIndexMutationShardCount>
      max_index_mutation_ts_{};
  std::atomic<int> pending_txn_count_{0};
  mutable std::array<VersionShard, kVersionShardCount> version_shards_;
  std::unordered_map<txn_id_t, uint64_t> active_snapshots_;
  PageManager* const page_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  mutable std::mutex transaction_table_lock;
  std::atomic<bool> synchronous_commit_{true};
  std::atomic<bool> metrics_enabled_{false};
  std::atomic<uint64_t> wal_wait_count_{0};
  std::atomic<uint64_t> wal_wait_ns_{0};
  std::atomic<uint64_t> write_intent_attempts_{0};
  std::atomic<uint64_t> write_intent_conflicts_{0};
  std::atomic<uint64_t> write_intent_mutex_wait_ns_{0};
  std::atomic<uint64_t> commit_shard_mutex_wait_ns_{0};

  // Background GC (declared last so the worker thread only starts after
  // every member it may touch is fully constructed).
  std::atomic<uint64_t> commits_since_gc_{0};
  std::atomic<bool> gc_stop_{false};
  std::mutex gc_mutex_;
  std::condition_variable gc_cv_;
  std::thread gc_worker_;

  // ---- deadlock policy & wait-for graph ----
  std::atomic<uint8_t> deadlock_policy_{
      static_cast<uint8_t>(DeadlockPolicy::kLegacy)};
  // Active deadlock detector thread (kDeadlockDetect only).
  std::atomic<bool> deadlock_detector_stop_{false};
  std::thread deadlock_detector_;
  // Wait-for graph for kDeadlockDetect: maps (waiting_txn -> holder_txn) per
  // row. A cycle in this graph is a deadlock. Maintained under a single
  // global mutex because deadlock detection is a rare event, not a hot path.
  mutable std::mutex wait_for_mu_;
  // Each entry: waiter_id -> (RowPosition, holder_id). When a waiter is
  // granted, its entry is removed; when a holder commits/aborts, every entry
  // whose holder_id matches is dropped.
  struct WaitForEdge {
    RowPosition row;
    txn_id_t holder;
  };
  std::unordered_map<txn_id_t, WaitForEdge> wait_for_edges_;
  // Background detector body: scans wait_for_edges_ for cycles and aborts the
  // youngest participant in each cycle.
  void DeadlockDetectorLoop();
  // Signal the detector to wake immediately (used by AddWaitForEdge and
  // when a transaction becomes unblocked, so an aborted victim releases
  // waiters quickly).
  std::mutex deadlock_detector_mu_;
  std::condition_variable deadlock_detector_cv_;
  // Records (waiting_txn, holder_txn, row) into the wait-for graph. Called
  // under the version shard's mutex by AcquireWriteIntent. No-op for
  // policies that handle ordering without a graph.
  void AddWaitForEdge(txn_id_t waiter, txn_id_t holder, RowPosition row);
  // Removes a waiter's edge when the lock is granted, the waiter times out,
  // or the waiter is aborted.
  void RemoveWaitForEdge(txn_id_t waiter);
  // Removes every edge whose holder matches (used on commit/abort so the
  // graph cannot keep dead references to terminated transactions).
  void RemoveWaitForEdgesOf(txn_id_t holder);
  // Out-of-band victim selection: returns true when a waiter's strict wait
  // should actually be "die" (Wait-Die) or "wound" (Wound-Wait). The
  // policies are encoded here so the call site stays a one-liner.
  bool ShouldAbortOnWait(txn_id_t waiter, txn_id_t holder) const;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_MANAGER_HPP
