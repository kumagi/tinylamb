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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"

namespace tinylamb {

struct TransactionRuntimeStats {
  uint64_t wal_sync_count{0};
  uint64_t wal_sync_ns{0};
  uint64_t wal_buffered_bytes{0};
  uint64_t wal_durable_bytes{0};
  uint64_t wal_wait_count{0};
  uint64_t wal_wait_ns{0};
  uint64_t write_intent_attempts{0};
  uint64_t write_intent_conflicts{0};
  // Times the latch-cycle escape hatch fired: a waiter gave up after
  // GetWriteIntentWaitLimit() instead of sleeping forever. Each occurrence
  // is a multi-second stall, so this gauge stays near zero on a healthy
  // deployment; a climbing value means latch-owning waits need attention.
  uint64_t write_intent_wait_timeouts{0};
  // Times a waiter gave up because the intent HOLDER had started aborting
  // (Transaction::IsAbortStarted). The holder's undo walk may need the page
  // latch the waiter is holding, so only the waiter can break that cycle;
  // detecting it via the abort flag resolves it in one poll tick (~10ms)
  // instead of riding the multi-second wait limit. A healthy deployment
  // shows this near zero; a climbing value means aborts and write waits
  // interleave (heavy conflict on rows whose pages are latched mid-wait).
  uint64_t write_intent_holder_abort_giveups{0};
  uint64_t write_intent_mutex_wait_ns{0};
  uint64_t commit_shard_mutex_wait_ns{0};
  // Long-uptime observability: a snapshot age that keeps growing while
  // version_chains climbs means MVCC GC is pinned by a live transaction
  // (see GarbageCollectVersions' pin warning).
  uint64_t active_transactions{0};
  uint64_t version_chains{0};
  uint64_t oldest_snapshot_ts{0};
  uint64_t current_commit_ts{0};
  // Pages with an outstanding index-undo reservation. Bounded by the write
  // sets of active transactions; growth while active_transactions is zero
  // means a reservation release path leaked (a leaked page permanently loses
  // its reserved bytes from insert admission).
  uint64_t index_undo_reserved_pages{0};
  // Per-index mutation gate (AcquireIndexWriteGuard) contention, sampled:
  // only acquisitions that blocked >1ms accumulate, so the uncontended RMW
  // majority pays no extra atomic traffic. index_gate_long_waits counts
  // acquisitions that blocked >1s -- a sign a single RMW (foster absorption
  // chain, split storm) is serializing a whole index; that is the
  // precondition measurement for a gate-free RMW rebuild.
  uint64_t index_gate_contended_acquires{0};
  uint64_t index_gate_wait_ns{0};
  uint64_t index_gate_long_waits{0};
  // Table-tail growth waits (ReserveTableTail / WaitForTableTailPage),
  // sampled >1ms. Tail ownership resolves at the grower's commit, so a
  // climbing total with a low count = one slow grower convoying the table's
  // appends; a high count = repeated page-boundary convoys.
  uint64_t table_tail_wait_count{0};
  uint64_t table_tail_wait_ns{0};
  // Rows served by the index-only visibility-map fast path: a non-retained
  // index entry on a page with no version chains is answered from the
  // B+Tree entry itself (key + INCLUDE payload) without the heap fetch
  // (see IndexScanIterator::TryReadIndexOnly).  A/B hatch:
  // TINYLAMB_DISABLE_INDEX_ONLY_VISIBILITY_MAP=1.
  uint64_t index_only_visible_serves{0};
  // Row pages warmed by the index-scan prefetcher (a second iterator runs a
  // fixed distance ahead of the scan and fetches each upcoming page into the
  // buffer pool, so the scan's own fetch is a pool hit).  A/B hatch:
  // TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH=1.
  uint64_t index_scan_prefetch_pages{0};
};

class Logger;
class PageManager;
class Transaction;
class RecoveryManager;
enum class TransactionStatus : uint_fast8_t;
struct LogRecord;
struct RowPosition;

class TransactionManager {
 public:
  // Non-copyable by design: instances are owned by smart pointers and
  // referenced by raw pointers throughout the executor/graph object web.
  TransactionManager(const TransactionManager&) = delete;
  TransactionManager& operator=(const TransactionManager&) = delete;
  TransactionManager(TransactionManager&&) = delete;
  TransactionManager& operator=(TransactionManager&&) = delete;
  TransactionManager(PageManager* pm, Logger* l, RecoveryManager* r)
      : page_manager_(pm), logger_(l), recovery_(r) {
    write_intent_wait_limit_ms_.store(WriteIntentWaitLimitMsFromEnv(),
                                      std::memory_order_release);
    StartGcWorker();
  }

  // Stops the background GC worker.  Must not race other TransactionManager
  // calls (same teardown contract as the rest of the storage stack).
  ~TransactionManager();

  Transaction Begin(bool read_only = false);

  // Recovery seeding: transaction ids must never be reused across a crash.
  // Recovery classifies losers by "no kCommit with this txn_id in the WAL",
  // so a fresh counter starting at 1 after restart lets an OLD commit record
  // vouch for a NEW uncommitted transaction, whose writes then skip undo
  // (fuzz-found: an uncommitted leaf DELETE survived recovery because a
  // previous lifetime's txn 1 had committed). Call with max_seen_id + 1.
  void SeedNextTransactionId(txn_id_t next_id) {
    txn_id_t current = next_txn_id_.load(std::memory_order_relaxed);
    while (current < next_id &&
           !next_txn_id_.compare_exchange_weak(current, next_id,
                                               std::memory_order_relaxed)) {
    }
  }

  // With a target pointer, publish/finish the transaction and return the
  // explicit own+dependency barrier to the caller instead of waiting here.
  // No externally visible reply may precede WaitForWALDurable(*target).
  Status PreCommit(Transaction& txn,
                   lsn_t* external_durability_target = nullptr);

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
  //   kNoWait        -- never wait: a conflicting writer fails at once (the
  //                     try-lock path), so the caller aborts and retries.
  //                     Required when every transaction executes on ONE
  //                     thread (the PostgreSQL server's event loop): there the
  //                     holder can only make progress after the waiter
  //                     returns, so any wait is a self-deadlock that rides the
  //                     5 s escape hatch and then fails anyway.
  enum class DeadlockPolicy : uint8_t {
    kLegacy = 0,
    kWaitDie = 1,
    kWoundWait = 2,
    kDeadlockDetect = 3,
    kNoWait = 4,
  };
  void SetDeadlockPolicy(DeadlockPolicy policy) {
    deadlock_policy_.store(static_cast<uint8_t>(policy),
                           std::memory_order_release);
    // Two concurrent callers could both observe a non-joinable detector and
    // move-assign the thread twice, which terminates the process.
    if (policy == DeadlockPolicy::kDeadlockDetect) {
      std::scoped_lock lock(deadlock_detector_mutex_);
      if (!deadlock_detector_.joinable()) {
        try {
          deadlock_detector_ = std::thread([this] { DeadlockDetectorLoop(); });
        } catch (...) {
          deadlock_policy_.store(
              static_cast<uint8_t>(DeadlockPolicy::kWoundWait),
              std::memory_order_release);
        }
      }
    }
  }
  [[nodiscard]] DeadlockPolicy GetDeadlockPolicy() const {
    return static_cast<DeadlockPolicy>(
        deadlock_policy_.load(std::memory_order_acquire));
  }
  // Safety valve for write-intent waits: a waiter that exceeds this limit
  // gives up with kConflicts instead of sleeping forever.  The holder may be
  // deadlocked on the WAITER's page latch (RowPage ops run exclusively
  // latched; Abort's undo walk re-acquires that latch before AbortVersions
  // can release the intents), a cycle no detector can see.  Generous enough
  // that legitimate contention never hits it; test-tunable, and
  // TINYLAMB_WRITE_INTENT_WAIT_MS overrides the default at startup for
  // deployments whose workload mix needs a different stall bound.
  void SetWriteIntentWaitLimit(std::chrono::milliseconds limit) {
    write_intent_wait_limit_ms_.store(static_cast<int64_t>(limit.count()),
                                      std::memory_order_release);
  }
  [[nodiscard]] std::chrono::milliseconds GetWriteIntentWaitLimit() const {
    return std::chrono::milliseconds(
        write_intent_wait_limit_ms_.load(std::memory_order_acquire));
  }
  // Upper bound for the durability-stall extension of the write-intent
  // escape hatch (see ExtendIntentDeadlineForDurabilityBarrier).  The twin
  // of LockManager::kDurabilityWaitFloor: a stalled fsync must not turn
  // innocent waiters into spurious latch-cycle victims, but a genuine cycle
  // still cannot outwait this bound.
  static constexpr std::chrono::milliseconds kDurabilityIntentWaitFloor{60'000};
  // Marks the caller as blocked on a WAL durability barrier (commit fsync,
  // write-back durability gate).  While any barrier is in flight the
  // write-intent escape hatch stretches toward kDurabilityIntentWaitFloor:
  // the dominant holder-slowed-by-I/O case is not a latch cycle, and timing
  // it out produced abort cascades whenever a checkpoint's data-file sync
  // stretched group-commit latency.  Prefer the RAII DurabilityBarrierGuard.
  void BeginDurabilityBarrier() noexcept {
    durability_barrier_waits_.fetch_add(1, std::memory_order_acq_rel);
  }
  void EndDurabilityBarrier() noexcept {
    durability_barrier_waits_.fetch_sub(1, std::memory_order_acq_rel);
  }
  class [[nodiscard]] DurabilityBarrierGuard {
   public:
    explicit DurabilityBarrierGuard(TransactionManager& tm) : tm_(&tm) {
      tm_->BeginDurabilityBarrier();
    }
    ~DurabilityBarrierGuard() { tm_->EndDurabilityBarrier(); }
    DurabilityBarrierGuard(const DurabilityBarrierGuard&) = delete;
    DurabilityBarrierGuard& operator=(const DurabilityBarrierGuard&) = delete;
    DurabilityBarrierGuard(DurabilityBarrierGuard&&) = delete;
    DurabilityBarrierGuard& operator=(DurabilityBarrierGuard&&) = delete;

   private:
    TransactionManager* tm_;
  };
  // WaitForDurable with the durability-barrier bookkeeping the write-intent
  // wait loops consult.  Every engine-side fsync wait should go through this
  // (PreCommit barriers, write-back gates) so a stalled disk reads as "the
  // system is slow", not "my holder is deadlocked".
  Status WaitForWALDurable(lsn_t lsn);
  [[nodiscard]] TransactionRuntimeStats RuntimeStats() const;
  [[nodiscard]] bool FailStopped() const {
    return fail_stop_.load(std::memory_order_acquire);
  }

  Status Abort(Transaction& txn);

  StatusOr<lsn_t> CompensateUpdateLog(txn_id_t txn_id, page_id_t pid,
                                      slot_t slot, std::string_view redo,
                                      lsn_t undo_next_lsn = 0);
  // Non-waiting first-updater-wins reservation stored in the same shard as
  // the row's MVCC chain. A stale snapshot or another pending writer loses.
  // When `before` is supplied (update/delete of an existing row), the chain's
  // base committed version is installed while the shard mutex is held so
  // concurrent readers see the old image for the whole intent window instead
  // of kNotExists.
  bool AcquireWriteIntent(
      Transaction& txn, const RowPosition& rp, bool wait,
      std::optional<std::string_view> before = std::nullopt);
  [[nodiscard]] bool HasForeignWriteIntent(const Transaction& txn,
                                           const RowPosition& rp) const;
  Status WaitForRowWriteIntent(Transaction& txn, const RowPosition& rp);

  [[nodiscard]] uint64_t CurrentCommitTimestamp() const {
    return commit_timestamp_.load();
  }
  // Highest timestamp whose every version is published and therefore visible
  // to a new snapshot.  Begin() takes snapshots here instead of from
  // commit_timestamp_, whose latest allocation may still be mid-publication.
  [[nodiscard]] uint64_t StableTimestamp() const {
    return stable_timestamp_.load(std::memory_order_acquire);
  }
  // resolve_head=false keeps a caller-owned UNSTAGED write intent from
  // upgrading the read to the newest committed version: snapshot reads
  // (SELECT scans) use it so a scan-side intent taken by an earlier DML
  // statement cannot leak a post-snapshot commit into plain reads.  Write
  // paths keep the default so a writer that waited on an intent resolves
  // the predecessor's newest committed image.
  //
  // Returns nullopt when the caller's latched page image is the
  // authoritative version: the value equals *physical byte for byte, so the
  // caller may serve a view of the image instead of paying a string copy
  // per read (full scans of chain-bearing pages otherwise allocate and copy
  // every row body twice -- here and into the read cache).
  StatusOr<std::optional<std::string>> ReadVersion(
      const Transaction& txn, const RowPosition& rp,
      std::optional<std::string_view> physical, bool resolve_head = true) const;
  StatusOr<std::optional<std::string>> ReadLatestVersion(
      const Transaction& txn, const RowPosition& rp,
      std::optional<std::string_view> physical) const;
  // True when any version chain entry exists for the row.  Without one, the
  // physical row image is the only version and is visible to every snapshot,
  // letting readers skip the copy/cache slow path in Transaction::ReadVersion.
  [[nodiscard]] bool HasVersionChain(const RowPosition& rp) const;
  // Reports whether any row on |pid| has a version chain. Exact per page:
  // only pages whose rows actually carry a chain answer true.
  [[nodiscard]] bool HasPageVersionChains(page_id_t pid) const;
  // Reports whether any page in any shard currently has a version chain.
  // Allows full-table scans on read-mostly / freshly-loaded tables to skip
  // per-page version-chain probes in O(shards) instead of O(pages).
  [[nodiscard]] bool HasAnyPageVersionChains() const noexcept {
    return std::ranges::any_of(page_chain_counts_, [](const auto& count) {
      return count.load(std::memory_order_acquire) != 0;
    });
  }
  void SweepCommittedVersionChains() {
    for (size_t i = 0; i < kGcPassDivisor; ++i) {
      GarbageCollectVersions();
    }
  }
  // Counts one row served by the index-only visibility-map fast path (see
  // TransactionRuntimeStats::index_only_visible_serves).
  void RecordIndexOnlyVisibleServe() const {
    index_only_visible_serves_.fetch_add(1, std::memory_order_relaxed);
  }
  // Counts one row page warmed by the index-scan prefetcher (see
  // TransactionRuntimeStats::index_scan_prefetch_pages).
  void RecordIndexScanPrefetch() const {
    index_scan_prefetch_pages_.fetch_add(1, std::memory_order_relaxed);
  }
  // Drops every version chain keyed to |pid|.  RowPosition namespaces chain
  // keys by {page_id, slot} only, so a page id recycled off the free list
  // would otherwise serve the previous incarnation's committed rows to scans
  // of the new page (they surface for physically vacant slots, and to
  // snapshots predating the new rows for occupied ones).  Called by
  // PageManager::AllocateNewPage when a destroyed page id is reused; an
  // aborted destroy restores its page in place and never recycles the id,
  // so the destroyed incarnation's history survives for old snapshots.
  // |reclaimer| (the allocating transaction) also drops the dead
  // incarnation's slots from its own write set, so a later write re-acquires
  // a fresh write intent under the new incarnation instead of skipping the
  // reservation on a stale write-set entry.
  void InvalidatePageVersions(page_id_t pid, Transaction* reclaimer);
  void CommitRecycledPageAllocation(Transaction& txn, page_id_t pid);
  // Index-page undo space reservation. A logged leaf/branch mutation whose
  // UNDO re-consumes the bytes it freed (delete re-insert, shrink update,
  // fence/foster restore) must keep those bytes out of OTHER transactions'
  // reach until the mutator resolves: a concurrent insert admitted on the
  // freed accounting makes the undo physically impossible (kNoSpace),
  // fail-stops the runtime abort, and -- because restart recovery replays the
  // same undo against the same page -- wedges the open forever. This is the
  // Leaf/Branch twin of the RowPage hole reservation: pages stay
  // format-unchanged; the reservation lives here, keyed by (page, owner), and
  // is released when the transaction leaves the active table. A transaction
  // never discounts its OWN reservation: the reverse-chain undo frees the
  // later ops' bytes before the earlier ops' undo re-claims them.
  void ReserveIndexUndoBytes(Transaction& txn, page_id_t pid, size_t bytes);
  // Diagnostic bisect for the reservation machinery (env
  // TINYLAMB_DISABLE_INDEX_RESERVATION): 0=on, 1=off, 2=no shared map,
  // 3=no per-transaction vector.
  static int ReservationBisectMode();
  // Bytes on |pid| that transactions OTHER than |self| may still claim for
  // their undo. Admission checks subtract this from free_size_ (a hash hit
  // only shrinks usable space; it can never grant space that is not there).
  [[nodiscard]] size_t IndexUndoReservedBytes(page_id_t pid,
                                              txn_id_t self) const;
  // Waits until no OTHER transaction holds an index-undo reservation on
  // |pid| (ending early when they resolve; kConflicts at |limit|).
  // LATCH-FREE by contract: the caller must not hold the page's latch,
  // because a reservation owner's remaining statements may need that very
  // latch -- waiting latched would rebuild the latch cycle the write-intent
  // escape hatch exists for. This is what turns a spurious kNoSpace (a
  // concurrent deleter's undo reservation, released at its commit/abort)
  // into a short productive wait instead of a failed statement.
  Status WaitForIndexReservationDrain(page_id_t pid, txn_id_t self,
                                      std::chrono::milliseconds limit) const;
  // True when ANY page carries a reservation from a transaction other than
  // |self|. Cheap: only pages with in-flight mutators appear.
  [[nodiscard]] bool HasForeignIndexReservations(txn_id_t self) const;
  // Waits until no transaction other than |self| holds any index-undo
  // reservation (kConflicts at |limit|). LATCH-FREE by contract -- see
  // WaitForIndexReservationDrain. The coarse form backs the tree-level
  // statement retry: a structural step (branch split, foster absorption)
  // refused on some page's reservation re-runs whole after the drain.
  Status WaitForAllIndexReservationsDrain(
      txn_id_t self, std::chrono::milliseconds limit) const;
  // Watcher-aware variant: slices the wait (wounded transactions bail at
  // the next slice instead of riding the full limit) and tags the stall
  // phase for the timeout postmortem. See ReserveTableTail's twin.
  Status WaitForAllIndexReservationsDrain(txn_id_t self,
                                          std::chrono::milliseconds limit,
                                          const Transaction* watcher) const;
  // Diagnostic: render outstanding reservation owners on |pid| as text.
  [[nodiscard]] std::string DebugDumpIndexReservations(page_id_t pid) const;
  // Diagnostic: every outstanding index-undo reservation EXCEPT |except|'s
  // own, with each owner's registry status and stall phase. Used by the
  // slow-drain warning; not on any hot path.
  [[nodiscard]] std::string DebugDumpAllIndexReservations(
      txn_id_t except) const;
  // True while |id| is registered in the active-transaction table (between
  // Begin's registration and the ForgetTransaction at the end of
  // commit/abort).
  [[nodiscard]] bool IsActive(txn_id_t id) const;
  // True when |id| names a live transaction that has started aborting.  The
  // registry lookup under transaction_table_lock keeps the Transaction*
  // dereference safe (the object cannot finish between the find and the
  // flag read).  Write-intent waiters call this to give up on an
  // aborting holder instead of riding the full wait limit -- see
  // Transaction::MarkAbortStarted.
  [[nodiscard]] bool IsAborting(txn_id_t id) const;
  void RegisterVersionWrite(Transaction& txn, const RowPosition& rp,
                            std::optional<std::string_view> before,
                            std::optional<std::string_view> after);
  // True when the committed chain at |rp| still serves a live row under
  // |txn|'s snapshot.  Insert slot-reuse consults this: reusing a position
  // whose pre-delete image is still snapshot-visible would mask that row
  // behind the inserter's staged value.
  [[nodiscard]] bool SnapshotSeesRow(const Transaction& txn,
                                     const RowPosition& rp) const;
  // Drops |txn|'s unstaged pending intent on |rp| and wakes waiters; used
  // when an acquired insert slot turns out to be snapshot-occupied.
  // Returns false when nothing was released (foreign or already-staged
  // intent).
  bool ReleaseWriteIntent(Transaction& txn, const RowPosition& rp);
  // Diagnostic: render every version chain keyed on |pid| as text
  // ("{p,s} committed:[{b,e,row|null}] pending:owner/staged").  Used by
  // session-fuzzer mismatch reports; not on any hot path.
  [[nodiscard]] std::string DebugDumpVersionChains(page_id_t pid) const;
  [[nodiscard]] bool RequiresHistoricalRead(const Transaction& txn) const;
  [[nodiscard]] bool IndexKeysMayBeStale(const Transaction& txn) const;
  [[nodiscard]] bool IndexKeysMayBeStale(const Transaction& txn,
                                         page_id_t index_root) const;

  class IndexReadGuard {
   public:
    IndexReadGuard() = default;
    explicit IndexReadGuard(std::shared_mutex& mutex);
    static IndexReadGuard Borrowed() noexcept {
      IndexReadGuard g;
      g.borrowed_ = true;
      return g;
    }
    IndexReadGuard(IndexReadGuard&& other) noexcept;
    IndexReadGuard& operator=(IndexReadGuard&& other) noexcept;
    IndexReadGuard(const IndexReadGuard&) = delete;
    IndexReadGuard& operator=(const IndexReadGuard&) = delete;
    ~IndexReadGuard();
    [[nodiscard]] bool HoldsLock() const noexcept {
      return lock_.owns_lock() || borrowed_;
    }

   private:
    friend class TransactionManager;
    std::shared_lock<std::shared_mutex> lock_;
    std::shared_mutex* tracked_mutex_{nullptr};
    bool borrowed_{false};
  };
  // Opaque encoded keys only: the index layer owns range semantics. The
  // caller must hold this root's read gate. nullopt keeps the coarse fallback.
  [[nodiscard]] std::optional<std::vector<std::string>>
  IndexKeyChangesSinceSnapshot(const Transaction& txn, page_id_t index_root,
                               const IndexReadGuard& guard) const;
  // Requires the root's write gate, like the physical key removal it records.
  void TrackIndexKeyMutation(txn_id_t owner, page_id_t index_root,
                             std::string_view key);
  // Compatibility callers that cannot supply the exact key revoke precision.
  // This is deliberately sticky for this manager; coarse epochs stay sound.
  void DisableIndexKeyTracking() {
    index_key_tracking_complete_.store(false, std::memory_order_release);
  }
  [[nodiscard]] IndexReadGuard AcquireIndexReadGuard(
      page_id_t index_root) const;
  [[nodiscard]] std::unique_lock<std::shared_mutex> AcquireIndexWriteGuard(
      page_id_t index_root) const;
  [[nodiscard]] std::vector<std::unique_lock<std::shared_mutex>>
  AcquireIndexWriteGuards(const std::vector<page_id_t>& index_roots) const;

  StatusOr<lsn_t> AddLog(const LogRecord& lr);
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
    // D4 (docs/design.md): the durable point of the commit record that made
    // this version visible (Logger::BufferedLSN() after the commit AddLog).
    // Readers record it as a dependency LSN and the commit path waits for it
    // to become durable before the result reaches the user.
    lsn_t commit_lsn{0};
  };
  struct PendingVersion {
    txn_id_t owner{0};
    std::thread::id owner_thread{};
    std::optional<std::string> value;
    bool staged{false};
  };
  struct VersionChain {
    std::vector<CommittedVersion> committed;
    std::optional<PendingVersion> pending;
  };
  // D4: commit_lsn is the durable point of this commit's WAL record; it is
  // stamped onto every published version so readers can register a
  // durability dependency on it.
  void CommitVersions(Transaction& txn, lsn_t commit_lsn);
  void AbortVersions(Transaction& txn);
  void ForgetTransaction(Transaction& txn);
  void GarbageCollectVersions();

  // ---- commit sequencing ----
  // Timestamps are handed out with a plain atomic fetch_add; versions are
  // then published under shard locks alone.  A freshly allocated timestamp is
  // registered as "unpublished" under the same mutex that allocates it, and
  // stable_timestamp_ only ever advances to (smallest unpublished - 1), so a
  // snapshot taken from it can never observe a half-published commit.
  [[nodiscard]] uint64_t AllocatePendingCommit();
  void PublishCommit(uint64_t ts);
  // ---- background GC ----
  static constexpr uint64_t kGcCommitThreshold = 8;
  // A version space at or below this many chains is swept in one full pass
  // (the historical behavior; unit tests rely on prompt reclamation). Above
  // it, each pass sweeps a rotating window of shards so the per-pass shard
  // lock footprint stays bounded no matter how large the space grows: a
  // full scan of millions of chains every kGcCommitThreshold commits would
  // make GC interference with commit/read hot paths grow with database
  // size, forever. Full coverage cycles every kGcPassDivisor passes.
  static constexpr uint64_t kGcFullScanChainLimit = 8192;
  static constexpr size_t kGcPassDivisor = 4;
  // A snapshot this many commits behind the frontier while more than
  // kGcFullScanChainLimit chains exist trips the rate-limited GC-pin warning.
  static constexpr uint64_t kGcPinWarnAgeCommits = 100'000;
  void StartGcWorker();
  void GcWorkerLoop();
  void MaybeWarnGcPin(const std::optional<uint64_t>& oldest_snapshot,
                      txn_id_t oldest_snapshot_txn, uint64_t total_chains);

  // Transaction move operations use these to keep active_transactions_
  // pointing at the live object: Begin() registers the address of the
  // Transaction it returns, and a move must carry the registration over to
  // the new address instead of leaving a pointer to the moved-from object.
  void MoveActiveTransaction(Transaction* from, Transaction* to);

  static constexpr size_t kVersionShardCount = 256;
  static constexpr size_t kIndexMutationShardCount = 256;

  struct VersionShard {
    mutable std::mutex mutex;
    std::condition_variable write_intent_released;
    std::unordered_map<RowPosition, VersionChain> versions;
  };

  [[nodiscard]] static size_t VersionShardIndex(const RowPosition& rp) {
    return static_cast<size_t>((rp.page_id * 131ULL) + rp.slot) %
           kVersionShardCount;
  }
  [[nodiscard]] static size_t IndexMutationShardIndex(page_id_t root) {
    return static_cast<size_t>(root * 11400714819323198485ULL) %
           kIndexMutationShardCount;
  }
  static constexpr size_t kPageChainShardCount = 256;
  [[nodiscard]] static size_t PageChainShardIndex(page_id_t pid) {
    return static_cast<size_t>((pid * 131ULL) % kPageChainShardCount);
  }
  // Lock-free scan fast path: a zero shard counter proves the row's page has
  // no version chain anywhere in the shard, so its physical image is the
  // only version and ReadVersion can serve it without locking a shard.
  // ReadTransaction::ReadVersion uses this instead of a full HasVersionChain
  // probe (which would take the shard mutex just to confirm what the caller
  // resolves again in TransactionManager::ReadVersion).
  [[nodiscard]] bool VersionShardIdle(const RowPosition& rp) const {
    return page_chain_counts_[PageChainShardIndex(rp.page_id)].load(
               std::memory_order_acquire) == 0;
  }
  // Sharded outstanding index-undo reservations, keyed by exact page id and
  // owner. Only pages with in-flight mutators appear, so the maps stay tiny;
  // the mutex is a leaf lock (admission paths hold it under a page latch,
  // release paths hold no page latch).
  static constexpr size_t kIndexReservationShardCount = 64;
  struct IndexReservationShard {
    mutable std::mutex mutex;
    // Signaled whenever a reservation on this shard is released; the drain
    // wait (WaitForIndexReservationDrain) sleeps on it.
    mutable std::condition_variable released;
    std::unordered_map<page_id_t, std::unordered_map<txn_id_t, uint64_t>> bytes;
    // Page entries per owner, protected by mutex. The coarse foreign-owner
    // test must not walk every page reserved by a large index build.
    std::unordered_map<txn_id_t, size_t> owner_pages;
  };
  [[nodiscard]] static size_t IndexReservationShardIndex(page_id_t pid) {
    return static_cast<size_t>((pid * 131ULL) % kIndexReservationShardCount);
  }
  std::array<IndexReservationShard, kIndexReservationShardCount>
      index_reservations_;
  // Signaled after any reservation release for the coarse drain wait. The
  // notify takes this mutex alone (never nested with a shard mutex), so the
  // wait path's drain_mu_ -> shard-mutex order has no inverse.
  mutable std::mutex reservation_drain_mu_;
  mutable std::condition_variable reservation_drain_cv_;
  void ReleaseIndexUndoReservations(Transaction& txn);
  VersionChain& GetOrCreateVersionChain(VersionShard& shard,
                                        const RowPosition& rp);
  void ForgetVersionChain(const RowPosition& rp);
  void MarkPendingIndexMutation(page_id_t index_root);
  void CommitIndexMutations(Transaction& txn, uint64_t commit_ts);
  void ClearPendingIndexMutations(Transaction& txn);
  void ReleasePendingIndexMutation(size_t shard);
  void FinishIndexKeyChanges(txn_id_t owner, page_id_t index_root,
                             std::optional<uint64_t> commit_ts);

  // Per-page chain bookkeeping (page_chain_shards_). Called exactly when a
  // chain enters/leaves shard.versions, mirroring page_chain_counts_.
  void PageChainCreated(page_id_t pid);
  void PageChainForgotten(page_id_t pid);

  // Byte-sized state flags share one aligned block.
  std::atomic<bool> index_key_tracking_complete_{true};
  std::atomic<bool> synchronous_commit_{true};
  std::atomic<bool> fail_stop_{false};
  std::atomic<bool> metrics_enabled_{false};
  std::atomic<bool> gc_stop_{false};
  std::atomic<uint8_t> deadlock_policy_{
      static_cast<uint8_t>(DeadlockPolicy::kDeadlockDetect)};
  std::atomic<bool> deadlock_detector_stop_{false};

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
  std::array<std::atomic<uint64_t>, kIndexMutationShardCount>
      pending_index_mutations_{};
  std::atomic<uint64_t> pending_index_mutation_count_{0};
  mutable std::array<std::shared_mutex, kIndexMutationShardCount>
      index_gates_{};
  struct PendingIndexKeys {
    txn_id_t owner;
    page_id_t root;
    std::vector<std::string> keys;
  };
  struct CommittedIndexKey {
    page_id_t root;
    uint64_t commit_ts;
    std::string key;
  };
  // Protected by the corresponding EXISTING index gate, never a new latch.
  // Bounded history + an eviction timestamp: older snapshots simply fall back.
  struct IndexKeyHistory {
    bool precise{true};
    size_t pending_bytes{0}, pending_keys{0}, history_bytes{0};
    uint64_t forgotten_through_ts{0};
    std::vector<PendingIndexKeys> pending;
    std::deque<CommittedIndexKey> committed;
    void Disable() noexcept {
      precise = false;
      pending.clear();
      committed.clear();
      pending_bytes = pending_keys = history_bytes = 0;
    }
  };
  std::array<IndexKeyHistory, kIndexMutationShardCount> index_key_history_{};
  mutable std::array<VersionShard, kVersionShardCount> version_shards_;
  std::array<std::atomic<int64_t>, kPageChainShardCount> page_chain_counts_{};
  // Exact per-page version-chain presence, sharded by page id (the same
  // sharding as page_chain_counts_). The atomic counters above only bound a
  // whole shard ("some page here has a chain"), which poisons every page in
  // the shard: scans answered them with a per-row shard-mutex + RowPosition
  // probe. The per-page map answers "does THIS page have a chain" with one
  // small probe, so read-mostly pages stay on the physical-read fast path
  // while any chain exists anywhere in the shard. Mutations hold the shard
  // mutex as a leaf lock; it nests only under a VersionShard::mutex (see
  // docs/lock_order.md).
  struct PageChainShard {
    mutable std::mutex mutex;
    std::unordered_map<page_id_t, uint32_t> counts;
  };
  mutable std::array<PageChainShard, kPageChainShardCount> page_chain_shards_{};
  std::unordered_map<txn_id_t, uint64_t> active_snapshots_;
  PageManager* const page_manager_;
  Logger* const logger_;
  RecoveryManager* const recovery_;
  mutable std::mutex transaction_table_lock;
  // WAL chain serialization lives on Transaction::chain_mu_ (one mutex per
  // transaction); the former manager-wide chain_publish_mu_ serialized every
  // append of every transaction on a single contended lock.
  std::atomic<uint64_t> wal_wait_count_{0};
  std::atomic<uint64_t> wal_wait_ns_{0};
  // mutable: AcquireIndexWriteGuard is const and records sampled waits.
  mutable std::atomic<uint64_t> index_gate_contended_acquires_{0};
  mutable std::atomic<uint64_t> index_gate_wait_ns_{0};
  mutable std::atomic<uint64_t> index_gate_long_waits_{0};
  // Rows served by the index-only visibility-map fast path (see
  // RecordIndexOnlyVisibleServe).
  mutable std::atomic<uint64_t> index_only_visible_serves_{0};
  // Row pages warmed by the index-scan prefetcher (see
  // RecordIndexScanPrefetch).
  mutable std::atomic<uint64_t> index_scan_prefetch_pages_{0};
  std::atomic<uint64_t> write_intent_attempts_{0};
  std::atomic<uint64_t> write_intent_conflicts_{0};
  std::atomic<uint64_t> write_intent_mutex_wait_ns_{0};
  std::atomic<uint64_t> commit_shard_mutex_wait_ns_{0};

  // Background GC (declared last so the worker thread only starts after
  // every member it may touch is fully constructed).
  std::atomic<uint64_t> commits_since_gc_{0};
  // Rotating shard cursor for the incremental GC pass. Owned by the GC
  // worker thread alone; no synchronization needed.
  size_t gc_cursor_{0};
  std::mutex gc_mutex_;
  std::condition_variable gc_cv_;
  std::thread gc_worker_;
  // Rate limiting for MaybeWarnGcPin; steady_clock milliseconds.
  std::atomic<int64_t> last_gc_pin_warn_ms_{0};
  static constexpr std::chrono::milliseconds kGcPinWarnInterval{10 * 60 * 1000};

  // ---- deadlock policy & wait-for graph ----
  // Default is kDeadlockDetect: the wait-for graph detector resolves a
  // genuine deadlock in ~one detector tick (2 ms) by wounding the youngest
  // participant, while kLegacy leaves a cycle to the 5 s write-intent
  // escape hatch -- measured on TPC-C (W=2, 32 clients) that turns every
  // cycle into a 5-second stall plus a full undo walk, 12x more engine
  // aborts and ~30% less throughput than detection. The explicit kLegacy
  // value remains available for benchmarks that want the pre-policy
  // behavior.
  // Write-intent wait escalation limit; see SetWriteIntentWaitLimit.
  std::atomic<int64_t> write_intent_wait_limit_ms_{5'000};
  // Startup read of TINYLAMB_WRITE_INTENT_WAIT_MS (fallback: the 5 s
  // default above; 0 or garbage keeps the default).
  static int64_t WriteIntentWaitLimitMsFromEnv();
  // Rate limiting for the escape-hatch warning; steady_clock milliseconds.
  std::atomic<int64_t> last_intent_timeout_warn_ms_{0};
  static constexpr std::chrono::milliseconds kIntentTimeoutWarnInterval{60 *
                                                                        1000};
  std::atomic<uint64_t> write_intent_wait_timeouts_{0};
  // In-flight WAL durability barrier waits (commit fsyncs, write-back
  // gates); see BeginDurabilityBarrier.  Read by the write-intent wait
  // loops to distinguish a stalled disk from a deadlocked holder.
  std::atomic<int64_t> durability_barrier_waits_{0};
  // Holder-abort fast give-ups; see TransactionRuntimeStats for the meaning.
  std::atomic<uint64_t> write_intent_holder_abort_giveups_{0};
  // Active deadlock detector thread (kDeadlockDetect only).
  std::thread deadlock_detector_;
  // Serializes detector spawn so concurrent SetDeadlockPolicy callers cannot
  // double-assign the thread (which would call std::terminate).
  mutable std::mutex deadlock_detector_mutex_;
  // Wait-for graph for kDeadlockDetect: maps (waiting_txn -> holder_txn) per
  // row. A cycle in this graph is a deadlock. Maintained under a single
  // global mutex because deadlock detection is a rare event, not a hot path.
  mutable std::mutex wait_for_mu_;
  // Each entry: waiter_id -> (RowPosition, holder_id). When a waiter is
  // granted, its entry is removed; when a holder commits/aborts, every entry
  // whose holder_id matches is dropped.
  struct WaitForEdge {
    RowPosition row;
    txn_id_t holder{};
  };
  std::unordered_map<txn_id_t, WaitForEdge> wait_for_edges_;
  // Cheap sleep predicate for the detector: updated under wait_for_mu_
  // together with the map, read relaxed by the detector. count == 0 implies
  // the map is empty, so the detector can block indefinitely instead of
  // polling the map every tick.
  std::atomic<int64_t> wait_for_edge_count_{0};
  // Bumped under wait_for_mu_ on every edge insert and removal; the detector
  // skips an analysis pass when this has not moved since the last one. Under
  // sustained contention waiters are always present, so the edge count alone
  // would make the detector re-run its O(V+E) pass back to back (a busy
  // spin); the generation distinguishes "waiters still waiting" from
  // "the graph changed".
  std::atomic<uint64_t> wait_for_generation_{0};
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
  // Pushes an expired escape-hatch deadline out instead of giving up, when
  // the holder's lack of progress is explained by an in-flight WAL
  // durability barrier or a group-commit backlog: those stall holders for
  // seconds without any deadlock, and timing out against them produced
  // abort cascades (TPC-C: bursts of 5 s escapes exactly while a
  // checkpoint's data-file sync was stretching every fsync).  Returns false
  // when no barrier is in flight or the kDurabilityIntentWaitFloor total
  // budget is spent -- only then is the give-up a real latch-cycle answer.
  [[nodiscard]] bool ExtendIntentDeadlineForDurabilityBarrier(
      std::chrono::steady_clock::time_point wait_begin,
      std::chrono::steady_clock::time_point* deadline) const;
  // Counts one holder-abort fast give-up (see TransactionRuntimeStats).
  void RecordHolderAbortGiveup();
  // Escape-hatch bookkeeping: bumps the timeout gauge and (rate-limited)
  // says which row stalled, so a deployment whose delivery-style deletes
  // keep tripping the latch cycle shows up in the log instead of surfacing
  // only as a latency tail.
  void RecordIntentWaitTimeout(const Transaction& txn, const RowPosition& rp);
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
