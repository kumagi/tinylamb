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

#include "transaction/transaction_manager.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {
class TransactionFinishGuard {
 public:
  TransactionFinishGuard(const TransactionFinishGuard&) = delete;
  TransactionFinishGuard& operator=(const TransactionFinishGuard&) = delete;
  TransactionFinishGuard(TransactionFinishGuard&&) = delete;
  TransactionFinishGuard& operator=(TransactionFinishGuard&&) = delete;
  explicit TransactionFinishGuard(Transaction& transaction)
      : transaction_(transaction) {}
  ~TransactionFinishGuard() { transaction_.FinalizeFinishHooks(); }

 private:
  Transaction& transaction_;
};

// Backstop poll for write-intent waits.  The cross-shard wound broadcasts
// (Wound-Wait and the deadlock detector) notify without the target shard's
// mutex -- acquiring every shard mutex there would need a global shard-lock
// order the code does not have -- so a waiter between its predicate check
// and its block can miss one notification.  Bounding the wait turns that
// from a permanent stall into a one-interval delay; a delivered notify
// still wakes the waiter immediately.
constexpr auto kWriteIntentPollInterval = std::chrono::milliseconds(10);

// A waiter only abandons an aborting holder after this long. Real aborts
// finish far below it (durability wait + undo walk of a handful of rows),
// so a waiter whose holder unwinds cleanly still receives the row it was
// waiting for; an abort that is STILL in flight after the delay is stuck
// -- typically on the page latch this very waiter holds (RowPage ops run
// exclusively latched) -- and only the waiter can break that cycle. The
// 5 s write-intent wait limit stays as the final backstop for cycles with
// no aborting participant at all.
constexpr auto kHolderAbortGiveupDelay = std::chrono::milliseconds(250);
}  // namespace

int64_t TransactionManager::WriteIntentWaitLimitMsFromEnv() {
  constexpr int64_t kDefaultMs = 5'000;
  const char* env = std::getenv("TINYLAMB_WRITE_INTENT_WAIT_MS");
  if (env == nullptr || env[0] == '\0') {
    return kDefaultMs;
  }
  char* end = nullptr;
  const long long parsed = std::strtoll(env, &end, 10);
  if (end == env || *end != '\0' || parsed <= 0) {
    return kDefaultMs;
  }
  return static_cast<int64_t>(parsed);
}

TransactionManager::~TransactionManager() {
  gc_stop_.store(true, std::memory_order_release);
  {
    std::scoped_lock wake(gc_mutex_);
  }
  gc_cv_.notify_all();
  if (gc_worker_.joinable()) {
    gc_worker_.join();
  }
  // Stop the deadlock detector if it was ever started. The scoped lock
  // serializes the wake against the detector's predicate check: without it a
  // notify could land between the check and the block, leaving the detector
  // asleep past shutdown.
  deadlock_detector_stop_.store(true, std::memory_order_release);
  {
    std::scoped_lock wake(deadlock_detector_mu_);
  }
  deadlock_detector_cv_.notify_all();
  if (deadlock_detector_.joinable()) {
    deadlock_detector_.join();
  }
  // Registered transactions may outlive this manager (a stack Transaction
  // held across a fixture reset, an aborted query unwound after database
  // teardown begins).  Null their manager pointers so the destructor of
  // each such Transaction is a no-op instead of dereferencing freed
  // memory; every still-live Transaction unregisters itself, so this map
  // only holds live objects here.
  {
    std::scoped_lock lk(transaction_table_lock);
    for (auto& [id, txn] : active_transactions_) {
      if (txn != nullptr) {
        txn->transaction_manager_ = nullptr;
      }
    }
  }
}

void TransactionManager::StartGcWorker() {
  try {
    gc_worker_ = std::thread([this] { GcWorkerLoop(); });
  } catch (const std::exception& error) {
    gc_stop_.store(true, std::memory_order_release);
    LOG(ERROR) << "failed to start transaction GC worker: " << error.what();
  } catch (...) {
    gc_stop_.store(true, std::memory_order_release);
    LOG(ERROR) << "failed to start transaction GC worker";
  }
  // The default policy is kDeadlockDetect, whose waiters rely on the
  // background detector to break cycles; without this eager start a manager
  // that is never re-configured would wait with detection semantics but no
  // detector -- every deadlock would ride the 5 s write-intent escape hatch.
  // On a spawn failure the policy falls back to kWoundWait, which resolves
  // most cycles without any background thread.
  std::scoped_lock lock(deadlock_detector_mutex_);
  if (GetDeadlockPolicy() == DeadlockPolicy::kDeadlockDetect &&
      !deadlock_detector_.joinable()) {
    try {
      deadlock_detector_ = std::thread([this] { DeadlockDetectorLoop(); });
    } catch (const std::exception& error) {
      LOG(ERROR) << "failed to start deadlock detector: " << error.what();
      deadlock_policy_.store(static_cast<uint8_t>(DeadlockPolicy::kWoundWait),
                             std::memory_order_release);
    } catch (...) {
      LOG(ERROR) << "failed to start deadlock detector";
      deadlock_policy_.store(static_cast<uint8_t>(DeadlockPolicy::kWoundWait),
                             std::memory_order_release);
    }
  }
}

void TransactionManager::AddWaitForEdge(txn_id_t waiter, txn_id_t holder,
                                        RowPosition row) {
  {
    std::scoped_lock lk(wait_for_mu_);
    const auto [it, inserted] = wait_for_edges_.try_emplace(
        waiter, WaitForEdge{.row = row, .holder = holder});
    it->second = WaitForEdge{.row = row, .holder = holder};
    if (inserted) {
      wait_for_edge_count_.fetch_add(1, std::memory_order_relaxed);
    }
    // Any edge change (insert or overwrite) may complete a cycle the last
    // analysis pass has not seen.
    wait_for_generation_.fetch_add(1, std::memory_order_release);
  }
  // Notify with the cv mutex held so the wake cannot slip between the
  // detector's predicate check and its block (the edge map itself is guarded
  // by wait_for_mu_; the atomic count keeps the predicate race-free).
  std::scoped_lock cv_lock(deadlock_detector_mu_);
  deadlock_detector_cv_.notify_all();
}

void TransactionManager::RemoveWaitForEdge(txn_id_t waiter) {
  std::scoped_lock lk(wait_for_mu_);
  if (wait_for_edges_.erase(waiter) != 0) {
    wait_for_edge_count_.fetch_sub(1, std::memory_order_relaxed);
    wait_for_generation_.fetch_add(1, std::memory_order_release);
  }
}

void TransactionManager::RemoveWaitForEdgesOf(txn_id_t holder) {
  // Fast path: no waiter anywhere means no edge can mention |holder|.  This
  // runs on EVERY PreCommit/Abort, so the uncontended case must not pay the
  // graph-mutex acquisition (the count only moves under the same mutex, so
  // a relaxed zero proves the map empty).
  if (wait_for_edge_count_.load(std::memory_order_relaxed) == 0) {
    return;
  }
  // Two ways a holder can disappear from the wait-for graph: it was the
  // waiter of someone else (clean up on grant), or it was the holder of
  // someone else's edge (clean up here when the holder commits/aborts).
  std::scoped_lock lk(wait_for_mu_);
  bool removed = false;
  for (auto it = wait_for_edges_.begin(); it != wait_for_edges_.end();) {
    if (it->second.holder == holder || it->first == holder) {
      it = wait_for_edges_.erase(it);
      wait_for_edge_count_.fetch_sub(1, std::memory_order_relaxed);
      removed = true;
    } else {
      ++it;
    }
  }
  if (removed) {
    wait_for_generation_.fetch_add(1, std::memory_order_release);
  }
}

void TransactionManager::DeadlockDetectorLoop() {
  // Edge-free sleep: the detector blocks until AddWaitForEdge's
  // mutex-serialized notification or shutdown wakes it. The atomic edge
  // count keeps the predicate free of wait_for_mu_, and the notification is
  // serialized under deadlock_detector_mu_, so the wake cannot slip between
  // the predicate check and the block. An idle database therefore pays
  // nothing for the detector; detection latency is the notify path itself.
  //
  // The wait predicate is "edges exist AND the graph changed since the last
  // pass", not just "edges exist": under sustained contention waiters are
  // almost always present, so a count-only predicate re-ran the O(V+E)
  // analysis back to back and the event-driven detector degenerated into a
  // busy spin (4.7% of a TPC-C profile). Graph mutations bump
  // wait_for_generation_, so a cycle formed after the last pass always
  // differs and wakes the pass immediately; a grant/timeout removing edges
  // with no new wait forming costs one timed wake per tick at most.
  //
  // The timed wait is only a backstop for generation bumps that carry no
  // notification (RemoveWaitForEdge on a grant) -- every cycle-forming edge
  // insert notifies under the cv mutex and wakes this loop at once. The
  // backstop therefore bounds nothing that matters for detection latency;
  // 25 ms instead of 2 ms keeps a database with a legitimately blocked
  // waiter (contended hot row) from waking this thread 500 times a second
  // to re-derive "still no cycle".
  constexpr auto kDetectorBackstop = std::chrono::milliseconds(25);
  uint64_t analyzed_generation = 0;
  // Reused across passes: the wait-for snapshot and the walk scratch. The
  // detector thread is their only user, so clear() (which keeps the bucket
  // arrays) keeps every pass allocation-free in steady state.
  std::unordered_map<txn_id_t, txn_id_t> waits_for;
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> already_wounded;
  std::vector<txn_id_t> path;
  while (!deadlock_detector_stop_.load(std::memory_order_acquire)) {
    {
      std::unique_lock<std::mutex> lk(deadlock_detector_mu_);
      deadlock_detector_cv_.wait_for(lk, kDetectorBackstop, [&] {
        return deadlock_detector_stop_.load(std::memory_order_acquire) ||
               (wait_for_edge_count_.load(std::memory_order_relaxed) > 0 &&
                wait_for_generation_.load(std::memory_order_acquire) !=
                    analyzed_generation);
      });
    }
    if (deadlock_detector_stop_.load(std::memory_order_acquire)) {
      break;
    }
    if (wait_for_edge_count_.load(std::memory_order_relaxed) == 0 ||
        wait_for_generation_.load(std::memory_order_acquire) ==
            analyzed_generation) {
      continue;
    }
    analyzed_generation = wait_for_generation_.load(std::memory_order_acquire);
    // Snapshot the current edges under the mutex, then analyze without it.
    // The inverse adjacency (holder -> waiting txn) is built once per pass so
    // the walk below is O(V + E), not a linear scan per hop.
    waits_for.clear();
    {
      std::scoped_lock g(wait_for_mu_);
      waits_for.reserve(wait_for_edges_.size());
      for (const auto& [w, e] : wait_for_edges_) {
        waits_for.emplace(w, e.holder);
      }
    }
    if (waits_for.empty()) {
      continue;
    }
    // For every waiter, walk the chain forward (waiter -> its holder -> that
    // holder's waiters' holders -> ...). When a holder equals the original
    // waiter, we have a cycle. Wound the maximum-id (youngest) participant.
    already_wounded.clear();
    for (const auto& [start, _] : waits_for) {
      visited.clear();
      path.clear();
      txn_id_t cur = start;
      bool found_cycle = false;
      for (int safety = 0; safety < 64; ++safety) {
        if (!visited.insert(cur).second) {
          // Found a cycle. Truncate path back to the first visit.
          auto dup = std::find(path.begin(), path.end(), cur);
          if (dup != path.end()) {
            path.erase(path.begin(), dup);
            found_cycle = true;
          }
          break;
        }
        path.push_back(cur);
        // cur is waiting for someone. That someone's holder chain continues
        // through the inverse adjacency.
        const auto it = waits_for.find(cur);
        if (it == waits_for.end()) {
          break;  // cur is not waiting; no follow-on.
        }
        cur = it->second;
      }
      if (!found_cycle) {
        continue;
      }
      txn_id_t victim = *std::max_element(path.begin(), path.end());
      if (already_wounded.insert(victim).second) {
        std::scoped_lock registry_lock(transaction_table_lock);
        auto found = active_transactions_.find(victim);
        if (found != active_transactions_.end() && found->second != nullptr) {
          found->second->Wound();
        }
        // Wound() only flips a flag: sleepers blocked in
        // AcquireWriteIntent's cv wait (possibly on a DIFFERENT shard than
        // the wounded holder's) never wake from the flag alone. Broadcast
        // every shard so the victim's own wait and every waiter blocked
        // behind the victim re-evaluate their predicates promptly.
        for (VersionShard& shard : version_shards_) {
          shard.write_intent_released.notify_all();
        }
      }
    }
  }
}

void TransactionManager::GcWorkerLoop() {
  bool retained_versions = false;
  while (!gc_stop_.load(std::memory_order_acquire)) {
    std::unique_lock<std::mutex> lk(gc_mutex_);
    // Hybrid idle strategy. ForgetTransaction notifies (under this mutex)
    // once the commit threshold is reached, so a busy database is served
    // event-driven; the 100 ms timed wait backs that up for the quiet case
    // where one or two commits land and no threshold notify ever fires --
    // the historical 10 ms poll drained any nonzero counter every tick, and
    // a purely event-driven wait would leave a single commit's chains
    // uncollected for its whole lifetime. The predicate re-reads the
    // counter after every pass, so commits that accumulated while a pass
    // ran are picked up without another notification. Below-threshold
    // bursts wait for the timed wake instead of making every completed
    // transaction trigger another full shard sweep.
    gc_cv_.wait_for(lk, std::chrono::milliseconds(100), [&] {
      return gc_stop_.load(std::memory_order_acquire) ||
             commits_since_gc_.load(std::memory_order_relaxed) >=
                 kGcCommitThreshold;
    });
    if (gc_stop_.load(std::memory_order_acquire)) {
      break;
    }
    lk.unlock();
    // A rotating pass may leave entire shards untouched. A snapshot or the
    // durability frontier can also advance without another writable commit.
    // Retry retained chains on the timed wake, rather than parking them
    // forever once a write burst gives way to read-only traffic. Retention is
    // deliberately absent from the wait predicate: pinned chains must not
    // turn this into a busy loop.
    if (commits_since_gc_.exchange(0, std::memory_order_relaxed) == 0 &&
        !retained_versions) {
      continue;
    }
    GarbageCollectVersions();
    retained_versions =
        std::ranges::any_of(page_chain_counts_, [](const auto& count) {
          return count.load(std::memory_order_relaxed) != 0;
        });
  }
}

Transaction TransactionManager::Begin(bool read_only) {
  txn_id_t new_txn_id = next_txn_id_.fetch_add(1);
  Transaction new_txn(new_txn_id, this, read_only);
  // No kBegin record: the undo walk follows prev_lsn_ chains rooted at each
  // write, and recovery classifies losers as "transactions whose newest LSN
  // has no matching kCommit". A per-transaction BEGIN record cost one WAL
  // append per transaction and carried no information the chains lack.
  {
    std::scoped_lock lk(transaction_table_lock);
    // The snapshot must be taken under the registry lock so a concurrent
    // GarbageCollectVersions (which computes the oldest active snapshot
    // under the same lock) can never trim versions this snapshot still
    // needs.  Reading stable_timestamp_ keeps Begin off the shard locks and
    // off the commit publication path entirely.
    new_txn.snapshot_ts_ = stable_timestamp_.load(std::memory_order_acquire);
    new_txn.DebugSetTid(static_cast<int64_t>(
        std::hash<std::thread::id>{}(std::this_thread::get_id())));
    active_transactions_.emplace(new_txn_id, &new_txn);
    active_snapshots_.emplace(new_txn_id, new_txn.snapshot_ts_);
  }
  assert(!new_txn.IsFinished());
  // If the return move-constructs instead of eliding into the caller,
  // Transaction's move operations repoint active_transactions_ at the new
  // address; the registration never outlives this local.
  return new_txn;
}

Status TransactionManager::PreCommit(Transaction& txn,
                                     lsn_t* external_durability_target) {
  if (external_durability_target != nullptr) {
    *external_durability_target = 0;
  }
  TransactionFinishGuard finish_guard(txn);
  if (txn.IsFinished()) {
    return Status::kRuntimeError;
  }
  if (txn.IsWounded()) {
    const Status abort_status = Abort(txn);
    return abort_status == Status::kSuccess ? Status::kConflicts : abort_status;
  }
  if (fail_stop_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  assert(!txn.IsFinished());
  // Drop every wait-for edge in which this transaction is the holder: the
  // pending intent is about to be released and any later waiter must not see
  // a stale holder reference. We do this before the WAL barrier so the
  // detector cannot try to wound us mid-commit.
  if (!txn.IsReadOnly()) {
    RemoveWaitForEdgesOf(txn.ID());
  }
  // D4 (docs/design.md): the commit record's AddLog completes BEFORE the
  // versions become visible ("never publish a version whose AddLog has not
  // completed"); releasing write intents may then happen without waiting for
  // fsync, while the user-visible result is gated by the durability barrier
  // at the end of this function (own commit LSN and every dependency LSN).
  // A write-capable context can still finish without changing anything
  // (WITH/EXPLAIN on the server's conservative execution path, for example).
  // WAL-before-data makes no log + no staged write/index/allocation work a
  // sufficient no-op proof. Version-only test/embedded writes still need a
  // commit record, even if they have not appended a data record themselves.
  const bool needs_commit_record =
      !txn.IsReadOnly() && (txn.HasLogRecord() || !txn.write_set_.empty() ||
                            !txn.mutated_index_roots_.empty() ||
                            !txn.recycled_page_allocations_.empty());
  if (needs_commit_record) {
    LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
    lsn_t commit_end = 0;
    bool append_failed = true;
    Status append_status = Status::kSuccess;
    {
      // The commit record append, the prev_lsn update, and the kCommitted
      // status flip move together under the transaction's OWN chain mutex.
      // The checkpoint's active-transaction snapshot takes the same mutex
      // per transaction, so it can never observe status==kRunning with a
      // prev_lsn whose commit record sits BELOW the checkpoint's begin LSN
      // -- a stale "running" entry like that made restart recovery classify
      // a durably committed transaction as a loser and silently roll it
      // back. (The commit record below begin_lsn is invisible to the
      // recovery scan, which starts at the checkpoint.) A per-transaction
      // mutex replaces the former manager-wide chain_publish_mu_, which
      // serialized every WAL append of every transaction on one lock.
      std::scoped_lock chain_guard(txn.chain_mu_);
      txn.serialization_scratch_.clear();
      commit_log.SerializeTo(txn.serialization_scratch_);
      StatusOr<lsn_t> appended = logger_->AddLog(txn.serialization_scratch_);
      if (appended.HasValue()) {
        txn.prev_lsn_ = appended.MoveValue();
        // AddLog returns the LSN *before* the payload; durable point is end
        // of the buffered commit record.
        commit_end = logger_->BufferedLSN();
        txn.SetStatus(TransactionStatus::kCommitted);
        append_failed = false;
      } else {
        // A dead logger cannot take compensation logs, so full rollback is
        // impossible.  The status flips to kAborted INSIDE the chain mutex
        // so the checkpoint snapshot cannot see a running transaction whose
        // commit record will never exist.  Registry/intent cleanup follows
        // outside the mutex: ForgetTransaction takes the transaction-table
        // lock, and the checkpoint nests chain locks below that same lock
        // (table lock -> chain mutex), so the reverse order here would
        // deadlock against a concurrent snapshot.
        if (page_manager_ != nullptr) {
          fail_stop_.store(true, std::memory_order_release);
        }
        append_status = appended.GetStatus();
        txn.SetStatus(TransactionStatus::kAborted);
      }
    }
    if (append_failed) {
      if (page_manager_ != nullptr && !txn.staged_records_.empty()) {
        page_manager_->ApplyTransactionAbort(txn.ID(), txn.staged_records_);
      }
      txn.staged_records_.clear();
      RemoveWaitForEdgesOf(txn.ID());
      AbortVersions(txn);
      ForgetTransaction(txn);
      return append_status;
    }
    try {
      for (page_id_t pid : txn.recycled_page_allocations_) {
        CommitRecycledPageAllocation(txn, pid);
      }
      txn.recycled_page_allocations_.clear();
      if (page_manager_ != nullptr && !txn.staged_records_.empty()) {
        page_manager_->ApplyTransactionCommit(txn.ID(), commit_end,
                                              txn.staged_records_);
      }
      txn.staged_records_.clear();
      CommitVersions(txn, commit_end);
      // The index-undo reservations protected this transaction's right to
      // ABORT; past CommitVersions the commit is irrevocable, no physical
      // undo will ever run, and a restart's loser-undo is single-threaded
      // against recovered state (no concurrent admissions to coordinate
      // with). Release them NOW instead of at ForgetTransaction: the
      // durability wait below can stretch under checkpoint fsyncs, and
      // every waiter parked in a reservation drain during that window held
      // row intents of its own (the tail of the TPC-C escape-hatch
      // cascades was exactly this: drain waits riding other transactions'
      // commit fsyncs).
      ReleaseIndexUndoReservations(txn);
      if (external_durability_target != nullptr) {
        // Both barriers participate explicitly; never assume the own commit
        // follows every dependency. Publication is already irrevocable, so
        // the finished transaction needs no registry slot during this wait.
        *external_durability_target =
            std::max(synchronous_commit_.load() ? commit_end : lsn_t{0},
                     txn.DurabilityDependence());
        txn.DebugSetPhase(nullptr);
        ForgetTransaction(txn);
        return Status::kSuccess;
      }
      Status durable = Status::kSuccess;
      txn.DebugSetPhase("commit-durable-wait");
      if (synchronous_commit_) {
        const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
        const auto wait_start = measure
                                    ? std::chrono::steady_clock::now()
                                    : std::chrono::steady_clock::time_point{};
        durable = WaitForWALDurable(commit_end);
        if (measure && durable == Status::kSuccess) {
          wal_wait_count_.fetch_add(1, std::memory_order_relaxed);
          wal_wait_ns_.fetch_add(
              static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now() - wait_start)
                      .count()),
              std::memory_order_relaxed);
        }
      }
      if (durable == Status::kSuccess) {
        txn.DebugSetPhase("dependency-durable-wait");
        // D4 barrier: the result this transaction produced depends on every
        // commit it observed, not only its own.  synchronous_commit may skip
        // the own-commit wait, but it never disables this dependency wait, and
        // a read-only transaction is subject to the same barrier.
        const lsn_t dependence = txn.DurabilityDependence();
        if (dependence != 0) {
          durable = WaitForWALDurable(dependence);
        }
      }
      txn.DebugSetPhase(nullptr);
      if (durable != Status::kSuccess) {
        // The versions are already published (CommitVersions above) and other
        // transactions may have read them and recorded D4 dependencies, so the
        // commit cannot be retracted: a failed fsync must not retroactively
        // un-commit visible data.  Keep kCommitted, release the registry slot,
        // and report the durability failure so the caller knows the commit may
        // not survive a crash.  Only the pre-publish AddLog-failure path above
        // may report aborted.
        RemoveWaitForEdgesOf(txn.ID());
        ForgetTransaction(txn);
        return durable;
      }
      ForgetTransaction(txn);
      return Status::kSuccess;
    } catch (const std::exception& error) {
      // A throw here means the publication/retry machinery could not finish
      // (typically memory exhaustion). The versions may already be visible,
      // so the commit stays kCommitted -- but the engine must stop and SAY
      // WHY: a silent fail-stop left a soak run without a single diagnostic
      // line to work from.
      LOG(ERROR) << "PreCommit failed mid-publication for transaction "
                 << txn.ID() << ": " << error.what();
      fail_stop_.store(true, std::memory_order_release);
      txn.SetStatus(TransactionStatus::kCommitted);
      RemoveWaitForEdgesOf(txn.ID());
      AbortVersions(txn);
      ForgetTransaction(txn);
      return Status::kRuntimeError;
    } catch (...) {
      LOG(ERROR) << "PreCommit failed mid-publication for transaction "
                 << txn.ID() << " with an unknown exception";
      fail_stop_.store(true, std::memory_order_release);
      txn.SetStatus(TransactionStatus::kCommitted);
      RemoveWaitForEdgesOf(txn.ID());
      AbortVersions(txn);
      ForgetTransaction(txn);
      return Status::kRuntimeError;
    }
  } else {
    txn.SetStatus(TransactionStatus::kCommitted);
    // D4 also applies to a write-capable context with no persistent work.
    // A transaction that read a
    // version published from a not-yet-durable commit must wait for that
    // commit LSN before its result is returned -- never skipped just because
    // there is no own commit record to flush.
    const lsn_t dependence = txn.DurabilityDependence();
    if (external_durability_target != nullptr) {
      *external_durability_target = dependence;
      txn.DebugSetPhase(nullptr);
      ForgetTransaction(txn);
      return Status::kSuccess;
    }
    if (dependence != 0) {
      txn.DebugSetPhase("ro-dependency-durable-wait");
      const Status durable = WaitForWALDurable(dependence);
      if (durable != Status::kSuccess) {
        // Nothing was published, but the registry slot and snapshot entry
        // must not outlive this stack-allocated transaction: a later
        // checkpoint, wounding, or deadlock scan would dereference freed
        // memory, and the orphaned snapshot would pin MVCC GC forever.
        RemoveWaitForEdgesOf(txn.ID());
        ForgetTransaction(txn);
        txn.SetStatus(TransactionStatus::kAborted);
        txn.DebugSetPhase(nullptr);
        return durable;
      }
    }
    txn.DebugSetPhase(nullptr);
    ForgetTransaction(txn);
    return Status::kSuccess;
  }
}

Status TransactionManager::Abort(Transaction& txn) {
  TransactionFinishGuard finish_guard(txn);
  // Guard double-finish: calling Abort() after a successful PreCommit must
  // not roll back already-published committed writes (the undo walk would
  // silently revert data other transactions may already have read).
  if (txn.IsFinished()) {
    return Status::kSuccess;
  }
  if (fail_stop_.load(std::memory_order_acquire)) {
    RemoveWaitForEdgesOf(txn.ID());
    AbortVersions(txn);
    txn.SetStatus(TransactionStatus::kAborted);
    ForgetTransaction(txn);
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  if (txn.IsReadOnly()) {
    txn.SetStatus(TransactionStatus::kAborted);
    RemoveWaitForEdgesOf(txn.ID());
    ForgetTransaction(txn);
    return Status::kSuccess;
  }
  // Announce the unwind BEFORE anything else: a waiter parked on this
  // transaction's write intent re-checks IsAborting on every poll tick and
  // gives up immediately instead of riding the multi-second wait limit.
  // This is what breaks the latch cycle -- the waiter typically holds a
  // page latch this undo walk needs (RowPage ops run exclusively latched),
  // so only the waiter can move first. Armed on every write path (fail-stop
  // included) so the flag never depends on which early return applies.
  txn.MarkAbortStarted();
  if (recovery_ == nullptr) {
    if (page_manager_ != nullptr && !txn.staged_records_.empty()) {
      page_manager_->ApplyTransactionAbort(txn.ID(), txn.staged_records_);
    }
    txn.staged_records_.clear();
    RemoveWaitForEdgesOf(txn.ID());
    AbortVersions(txn);
    txn.SetStatus(TransactionStatus::kAborted);
    ForgetTransaction(txn);
    if (txn.HasLogRecord()) {
      if (page_manager_ != nullptr) {
        fail_stop_.store(true, std::memory_order_release);
      }
      return Status::kRuntimeError;
    }
    return Status::kSuccess;
  }
  RemoveWaitForEdgesOf(txn.ID());
  Status undo_status = Status::kSuccess;
  try {
    txn.DebugSetPhase("abort-undo-walk");
    if (page_manager_ != nullptr && !txn.staged_records_.empty()) {
      page_manager_->ApplyTransactionAbort(txn.ID(), txn.staged_records_);
    }
    txn.staged_records_.clear();
    AbortVersions(txn);
    if (txn.HasLogRecord() && logger_ != nullptr) {
      LogRecord abort_log =
          LogRecord::AbortLogRecord(txn.prev_lsn_, txn.txn_id_);
      std::scoped_lock chain_guard(txn.chain_mu_);
      txn.serialization_scratch_.clear();
      abort_log.SerializeTo(txn.serialization_scratch_);
      StatusOr<lsn_t> appended = logger_->AddLog(txn.serialization_scratch_);
      if (appended.HasValue()) {
        txn.prev_lsn_ = appended.MoveValue();
        txn.SetStatus(TransactionStatus::kAborted);
      } else {
        undo_status = appended.GetStatus();
        if (page_manager_ != nullptr && recovery_ != nullptr) {
          fail_stop_.store(true, std::memory_order_release);
        }
        txn.SetStatus(TransactionStatus::kAborted);
      }
    } else {
      txn.SetStatus(TransactionStatus::kAborted);
    }
  } catch (const std::exception& error) {
    LOG(ERROR) << "Abort threw mid-undo for transaction " << txn.ID() << ": "
               << error.what();
    if (page_manager_ != nullptr && recovery_ != nullptr) {
      fail_stop_.store(true, std::memory_order_release);
    }
    try {
      AbortVersions(txn);
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
    txn.SetStatus(TransactionStatus::kAborted);
    undo_status = StatusError(StatusCode::kRuntimeError,
                              std::string("abort failed: ") + error.what());
  }
  txn.DebugSetPhase(nullptr);
  ForgetTransaction(txn);
  return undo_status;
}

TransactionManager::VersionChain& TransactionManager::GetOrCreateVersionChain(
    VersionShard& shard, const RowPosition& rp) {
  auto [it, inserted] = shard.versions.try_emplace(rp);
  if (inserted) {
    page_chain_counts_[PageChainShardIndex(rp.page_id)].fetch_add(
        1, std::memory_order_relaxed);
    PageChainCreated(rp.page_id);
  }
  return it->second;
}

void TransactionManager::ForgetVersionChain(const RowPosition& rp) {
  page_chain_counts_[PageChainShardIndex(rp.page_id)].fetch_sub(
      1, std::memory_order_relaxed);
  PageChainForgotten(rp.page_id);
}

void TransactionManager::PageChainCreated(page_id_t pid) {
  PageChainShard& shard = page_chain_shards_[PageChainShardIndex(pid)];
  std::scoped_lock lock(shard.mutex);
  ++shard.counts[pid];
}

void TransactionManager::PageChainForgotten(page_id_t pid) {
  PageChainShard& shard = page_chain_shards_[PageChainShardIndex(pid)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.counts.find(pid);
  if (found == shard.counts.end()) {
    return;
  }
  if (--found->second == 0) {
    shard.counts.erase(found);
  }
}

bool TransactionManager::AcquireWriteIntent(
    Transaction& txn, const RowPosition& rp, bool wait,
    std::optional<std::string_view> before) {
  if (fail_stop_.load(std::memory_order_acquire)) {
    txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kFailStopped);
    return false;
  }
  // A completed transaction must never install a fresh permanent intent: its
  // PreCommit/Abort short-circuit on IsFinished(), so nothing would ever
  // release the chain and the row would stay blocked forever. assert() in
  // Transaction::AddWriteSet vanishes under NDEBUG, so enforce it here, on the
  // single path every write-intent entry point funnels through.
  if (txn.IsFinished()) {
    txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kConflict);
    return false;
  }
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
  const auto wait_start = measure ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
  const DeadlockPolicy policy = GetDeadlockPolicy();
  std::unique_lock lock(shard.mutex);
  if (measure) {
    write_intent_attempts_.fetch_add(1, std::memory_order_relaxed);
    write_intent_mutex_wait_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - wait_start)
                .count()),
        std::memory_order_relaxed);
  }
  // An already-wounded transaction never makes progress: a deadlock detector
  // (or a Wound-Wait preemption) has marked it for abort. Return false so
  // AddWriteSet fails and the executor unwinds the transaction. Doing this
  // before touching the chain is essential: holding the intent while
  // wounded would block an arbitrary later commit.
  if (txn.IsWounded()) {
    txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
    return false;
  }
  auto available_for_self = [&] {
    const auto found = shard.versions.find(rp);
    return found == shard.versions.end() || !found->second.pending ||
           found->second.pending->owner == txn.ID();
  };
  auto holder_id = [&]() -> txn_id_t {
    const auto found = shard.versions.find(rp);
    if (found == shard.versions.end() || !found->second.pending) {
      return 0;
    }
    return found->second.pending->owner;
  };
  // Wait-clock state for the loops below, with PROGRESS RESET. A contended
  // hot row legitimately queues several waiters (TPC-C delivery: every
  // transaction in a district deletes the SAME oldest new_order row), and a
  // queued waiter must not hit the escape hatch just because its queue is
  // deep: every HANDOVER of the intent -- the holder we entered on commits or
  // aborts and someone else takes over -- is progress, so both the
  // escape-hatch deadline and the holder-abort grace restart from the
  // handover. A single holder that never moves still trips the hatch exactly
  // as before, which is the latch-cycle protection the limit exists for.
  auto wait_begin = std::chrono::steady_clock::now();
  auto intent_wait_deadline = wait_begin + GetWriteIntentWaitLimit();
  txn_id_t observed_holder = holder_id();
  // The holder has been unwinding for kHolderAbortGiveupDelay without
  // finishing: a live abort releases its intents in far less time, so this
  // abort is stuck -- typically on the page latch THIS waiter holds -- and
  // waiting longer only rides the multi-second escape hatch.
  auto holder_abort_stuck = [&] {
    return std::chrono::steady_clock::now() - wait_begin >=
               kHolderAbortGiveupDelay &&
           IsAborting(holder_id());
  };
  // A handover happened since we last looked: restart both clocks.
  // TINYLAMB_INTENT_NO_PROGRESS_RESET=1 restores the entry-time deadline
  // (diagnostic A/B: the escape hatch then counts wall clock from the first
  // wait, aborting deep-but-moving queues).
  static const bool kProgressResetDisabled =
      std::getenv("TINYLAMB_INTENT_NO_PROGRESS_RESET") != nullptr;
  auto check_holder_progress = [&]() {
    const txn_id_t current = holder_id();
    if (current != observed_holder) {
      observed_holder = current;
      if (policy == DeadlockPolicy::kDeadlockDetect && current != 0 &&
          current != txn.ID()) {
        AddWaitForEdge(txn.ID(), current, rp);
      }
      if (!kProgressResetDisabled) {
        wait_begin = std::chrono::steady_clock::now();
        intent_wait_deadline = wait_begin + GetWriteIntentWaitLimit();
      }
    }
  };
  // Try-lock path: caller (TryAddWriteSet) does not want to wait, regardless
  // of the policy, or the kNoWait policy forbids every wait. The current
  // pending owner is enough information to make the decision.
  if (!wait || policy == DeadlockPolicy::kNoWait) {
    if (available_for_self()) {
      VersionChain& chain = GetOrCreateVersionChain(shard, rp);
      if (chain.pending) {
        return chain.pending->owner == txn.ID();
      }
      chain.pending = PendingVersion{.owner = txn.ID(),
                                     .owner_thread = std::this_thread::get_id(),
                                     .value = std::nullopt,
                                     .staged = false};
      // Install the before-image as the base committed version while the
      // shard mutex is held: until RegisterVersionWrite stages the new
      // value, readers must see the old row instead of kNotExists.
      if (before && chain.committed.empty()) {
        chain.committed.push_back(
            {0, std::numeric_limits<uint64_t>::max(),
             std::optional<std::string>(std::string(*before))});
      }
      return true;
    }
    if (measure) {
      write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
    }
    txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kConflict);
    return false;
  }
  // Wait path: apply the configured deadlock policy.
  switch (policy) {
    case DeadlockPolicy::kLegacy: {
      // Pre-policy behavior: wait up to 5ms, then give up. Used for
      // benchmarks that do not want victim selection to influence results.
      auto available = [&] {
        const auto found = shard.versions.find(rp);
        return found == shard.versions.end() || !found->second.pending ||
               found->second.pending->owner == txn.ID() || txn.IsWounded();
      };
      if (!available() && !shard.write_intent_released.wait_for(
                              lock, std::chrono::milliseconds(5), available)) {
        if (measure) {
          write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
        }
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kTimeout);
        return false;
      }
      if (txn.IsWounded()) {
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
        return false;
      }
      break;
    }
    case DeadlockPolicy::kWaitDie: {
      // If we (the waiter) are younger than the holder, wait-die says we
      // must die.  We never wound a younger holder; we just refuse to wait
      // and let the caller abort us. This bounds wait-queue length to a
      // single older waiter per row and is deadlock-free by construction.
      const txn_id_t current_holder = holder_id();
      if (current_holder != 0 && current_holder != txn.ID() &&
          txn.ID() > current_holder) {
        if (measure) {
          write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
        }
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kConflict);
        return false;
      }
      auto available = [&] {
        if (txn.IsWounded()) {
          return true;
        }
        const auto found = shard.versions.find(rp);
        if (found == shard.versions.end() || !found->second.pending) {
          return true;
        }
        if (found->second.pending->owner == txn.ID()) {
          return true;
        }
        return false;  // Only let the wait continue while the holder is older
                       // (already checked) or gone. Re-check on each wake.
      };
      // Bounded wait: see kWriteIntentPollInterval for why a plain,
      // unbounded wait() could sleep through a cross-shard wound forever.
      txn.DebugSetPhase("intent-wait");
      while (!available()) {
        check_holder_progress();
        // The holder started aborting: its undo walk may need the page
        // latch THIS waiter holds, and its write is about to roll back, so
        // waiting is pointless and possibly a latch cycle. Give up at once.
        // Re-derive the owner: after a release the wait would continue only
        // against a NEW older holder, whose abort we must also catch.
        if (holder_abort_stuck()) {
          RecordHolderAbortGiveup();
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(
              Transaction::IntentWaitFailure::kHolderAborting);
          return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= intent_wait_deadline &&
            !ExtendIntentDeadlineForDurabilityBarrier(wait_begin,
                                                      &intent_wait_deadline)) {
          // Latch-cycle escape hatch: the holder may be a transaction whose
          // abort walk needs the page latch THIS waiter holds (page ops run
          // exclusively latched). Giving up unwinds us, releases the latch,
          // and lets the holder's AbortVersions make progress.
          RecordIntentWaitTimeout(txn, rp);
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kTimeout);
          return false;
        }
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      if (txn.IsWounded()) {
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
        return false;
      }
      break;
    }
    case DeadlockPolicy::kWoundWait: {
      // Older arriving transaction preempts a younger holder. We mark the
      // younger holder as wounded; the holder's next AcquireWriteIntent will
      // observe the wound and the executor will unwind. This is the classic
      // preemptive scheme and is well-suited to short TPC-C critical
      // sections where the wounded transaction has barely started.
      const txn_id_t current_holder = holder_id();
      if (current_holder != 0 && current_holder != txn.ID() &&
          txn.ID() < current_holder) {
        // Find the holder's Transaction* and wound it. The active registry
        // is the only place that holds the live pointer.
        std::scoped_lock registry_lock(transaction_table_lock);
        auto found = active_transactions_.find(current_holder);
        if (found != active_transactions_.end() && found->second != nullptr) {
          found->second->Wound();
        }
        // Nudge every shard waiter that might be holding an exclusive wait
        // on this transaction's wake-up chain. The shard waiters are not
        // aware of cross-shard wounds, so wounding a holder on another shard
        // would otherwise leave its own cv sleepers (and waiters queued
        // behind them) asleep forever.
        for (VersionShard& other : version_shards_) {
          other.write_intent_released.notify_all();
        }
      }
      auto available = [&] {
        if (txn.IsWounded()) {
          return true;
        }
        const auto found = shard.versions.find(rp);
        if (found == shard.versions.end() || !found->second.pending) {
          return true;
        }
        if (found->second.pending->owner == txn.ID()) {
          return true;
        }
        return false;
      };
      // Bounded wait: see kWriteIntentPollInterval for why a plain,
      // unbounded wait() could sleep through a cross-shard wound forever.
      txn.DebugSetPhase("intent-wait");
      while (!available()) {
        check_holder_progress();
        // Holder aborting: see the kWaitDie branch. The holder id can move
        // while we wait only after the intent is released (which makes
        // available() true), so checking the entry holder is sufficient.
        if (holder_abort_stuck()) {
          RecordHolderAbortGiveup();
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(
              Transaction::IntentWaitFailure::kHolderAborting);
          return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= intent_wait_deadline &&
            !ExtendIntentDeadlineForDurabilityBarrier(wait_begin,
                                                      &intent_wait_deadline)) {
          // Latch-cycle escape hatch: see the kWaitDie branch.
          RecordIntentWaitTimeout(txn, rp);
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kTimeout);
          return false;
        }
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      if (txn.IsWounded()) {
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
        return false;
      }
      break;
    }
    case DeadlockPolicy::kDeadlockDetect: {
      // Permit unrestricted waiting.  Register a wait-for edge so the
      // background detector can break any cycle it finds. The detector
      // either wounds this transaction (Wound()) or removes the edge when
      // the holder moves on.
      const txn_id_t current_holder = holder_id();
      if (current_holder != 0 && current_holder != txn.ID()) {
        // AddWaitForEdge wakes the detector under the cv mutex; a new edge
        // may have created a cycle.
        AddWaitForEdge(txn.ID(), current_holder, rp);
      }
      auto available = [&] {
        if (txn.IsWounded()) {
          return true;
        }
        const auto found = shard.versions.find(rp);
        if (found == shard.versions.end() || !found->second.pending) {
          return true;
        }
        if (found->second.pending->owner == txn.ID()) {
          return true;
        }
        return false;
      };
      // Bounded wait: see kWriteIntentPollInterval for why a plain,
      // unbounded wait() could sleep through a cross-shard wound forever.
      txn.DebugSetPhase("intent-wait");
      while (!available()) {
        check_holder_progress();
        // Holder aborting: see the kWaitDie branch. Re-derive the owner so
        // a chain whose intent moved to another pending writer (only after
        // a release we would have observed) still resolves promptly. The
        // wait-for edge registered at entry must go with us: a stale edge
        // would let the detector build cycles out of dead waits.
        if (holder_abort_stuck()) {
          RemoveWaitForEdge(txn.ID());
          RecordHolderAbortGiveup();
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(
              Transaction::IntentWaitFailure::kHolderAborting);
          return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= intent_wait_deadline &&
            !ExtendIntentDeadlineForDurabilityBarrier(wait_begin,
                                                      &intent_wait_deadline)) {
          // Latch-cycle escape hatch: see the kWaitDie branch. The wait-for
          // edge is moot once we give up.
          RemoveWaitForEdge(txn.ID());
          RecordIntentWaitTimeout(txn, rp);
          if (measure) {
            write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
          }
          txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kTimeout);
          return false;
        }
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      // Whether we got the lock or were wounded, our wait-for edge is moot.
      RemoveWaitForEdge(txn.ID());
      if (txn.IsWounded()) {
        txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
        return false;
      }
      break;
    }
    case DeadlockPolicy::kNoWait:
      // Unreachable: kNoWait takes the try-lock path above.
      break;
  }
  txn.DebugSetPhase(nullptr);
  VersionChain& chain = GetOrCreateVersionChain(shard, rp);
  if (chain.pending) {
    const bool ours = chain.pending->owner == txn.ID();
    // The write set may have pre-reserved this intent without a
    // before-image (TryAddWriteSet); the actual update now supplies one.
    // Install it so concurrent snapshots keep reading the pre-update value
    // instead of falling back to the (soon overwritten) heap image.
    if (ours && before && chain.committed.empty()) {
      chain.committed.push_back(
          {0, std::numeric_limits<uint64_t>::max(),
           std::optional<std::string>(std::string(*before))});
    }
    return ours;
  }
  // Strict write locking: a conflicting writer waits, then evaluates against
  // the latest physical row after reserving its own unstaged intent. This is
  // the TPC-C isolation behavior (T2 waits and completes), not SI's
  // first-updater-wins abort.
  chain.pending = PendingVersion{.owner = txn.ID(),
                                 .owner_thread = std::this_thread::get_id(),
                                 .value = std::nullopt,
                                 .staged = false};
  if (before && chain.committed.empty()) {
    chain.committed.push_back(
        {0, std::numeric_limits<uint64_t>::max(),
         std::optional<std::string>(std::string(*before))});
  }
  return true;
}

bool TransactionManager::HasForeignWriteIntent(const Transaction& txn,
                                               const RowPosition& rp) const {
  if (page_chain_counts_[PageChainShardIndex(rp.page_id)].load(
          std::memory_order_acquire) == 0) {
    return false;
  }
  const VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    return false;
  }
  const auto& pending = found->second.pending;
  if (!pending.has_value()) {
    return false;
  }
  return pending->owner != txn.ID();
}

Status TransactionManager::WaitForRowWriteIntent(Transaction& txn,
                                                 const RowPosition& rp) {
  if (fail_stop_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  if (txn.IsFinished()) {
    return Status::kConflicts;
  }
  if (txn.IsWounded()) {
    return Status::kConflicts;
  }
  if (GetDeadlockPolicy() == DeadlockPolicy::kNoWait) {
    return Status::kConflicts;
  }
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::unique_lock lock(shard.mutex);
  auto holder_id = [&]() -> txn_id_t {
    const auto found = shard.versions.find(rp);
    if (found == shard.versions.end() || !found->second.pending) {
      return 0;
    }
    return found->second.pending->owner;
  };
  const txn_id_t initial_holder = holder_id();
  if (initial_holder == 0 || initial_holder == txn.ID()) {
    return Status::kSuccess;
  }
  {
    const auto found = shard.versions.find(rp);
    if (found != shard.versions.end() && found->second.pending &&
        found->second.pending->owner_thread == std::this_thread::get_id()) {
      return Status::kDuplicates;
    }
  }
  {
    std::scoped_lock graph_lock(wait_for_mu_);
    txn_id_t cursor = initial_holder;
    for (size_t hops = 0; hops <= wait_for_edges_.size() && cursor != 0;
         ++hops) {
      if (cursor == txn.ID()) {
        return Status::kConflicts;
      }
      const auto edge = wait_for_edges_.find(cursor);
      cursor = edge == wait_for_edges_.end() ? 0 : edge->second.holder;
    }
  }
  AddWaitForEdge(txn.ID(), initial_holder, rp);
  auto wait_begin = std::chrono::steady_clock::now();
  auto intent_wait_deadline = wait_begin + GetWriteIntentWaitLimit();
  txn_id_t observed_holder = initial_holder;
  auto available = [&] {
    if (txn.IsWounded()) {
      return true;
    }
    const auto found = shard.versions.find(rp);
    if (found == shard.versions.end() || !found->second.pending) {
      return true;
    }
    return found->second.pending->owner == txn.ID();
  };
  txn.DebugSetPhase("unique-intent-wait");
  while (!available()) {
    const txn_id_t current = holder_id();
    if (current != observed_holder) {
      observed_holder = current;
      if (current != 0 && current != txn.ID()) {
        AddWaitForEdge(txn.ID(), current, rp);
      }
      wait_begin = std::chrono::steady_clock::now();
      intent_wait_deadline = wait_begin + GetWriteIntentWaitLimit();
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= intent_wait_deadline &&
        !ExtendIntentDeadlineForDurabilityBarrier(wait_begin,
                                                  &intent_wait_deadline)) {
      RemoveWaitForEdge(txn.ID());
      txn.DebugSetPhase(nullptr);
      RecordIntentWaitTimeout(txn, rp);
      txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kTimeout);
      return Status::kConflicts;
    }
    shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
  }
  RemoveWaitForEdge(txn.ID());
  txn.DebugSetPhase(nullptr);
  if (txn.IsWounded()) {
    txn.RecordIntentWaitFailure(Transaction::IntentWaitFailure::kWounded);
    return Status::kConflicts;
  }
  return Status::kSuccess;
}

Status TransactionManager::WaitForWALDurable(lsn_t lsn) {
  DurabilityBarrierGuard guard(*this);
  return logger_->WaitForDurable(lsn);
}

bool TransactionManager::ExtendIntentDeadlineForDurabilityBarrier(
    const std::chrono::steady_clock::time_point wait_begin,
    std::chrono::steady_clock::time_point* deadline) const {
  const bool barrier_in_flight =
      durability_barrier_waits_.load(std::memory_order_acquire) > 0;
  const lsn_t buffered = logger_->BufferedLSN();
  const lsn_t durable = logger_->DurableLSN();
  // A group-commit backlog means producers are outrunning fsyncs: holders
  // between statements stall on their next WAL append or page write-back
  // for as long as the pipeline needs to drain.  One-eighth of the ring
  // buffer is deep enough that only real pressure (not a quiet database)
  // trips it.
  const bool backlog =
      buffered > durable && buffered - durable > (lsn_t{1} << 20);
  if (!barrier_in_flight && !backlog) {
    return false;
  }
  const auto now = std::chrono::steady_clock::now();
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - wait_begin);
  if (elapsed >= kDurabilityIntentWaitFloor) {
    return false;
  }
  const auto extension =
      std::min(GetWriteIntentWaitLimit(), kDurabilityIntentWaitFloor - elapsed);
  *deadline = now + extension;
  return true;
}

void TransactionManager::RecordHolderAbortGiveup() {
  write_intent_holder_abort_giveups_.fetch_add(1, std::memory_order_relaxed);
}

// The escape hatch fired: this waiter held a page latch a blocked holder
// needed for its abort, so only giving up could break the cycle. Every
// occurrence is a multi-second stall, so the gauge matters and the row
// identity is worth a rate-limited log line.
void TransactionManager::RecordIntentWaitTimeout(const Transaction& txn,
                                                 const RowPosition& rp) {
  const uint64_t timeouts =
      write_intent_wait_timeouts_.fetch_add(1, std::memory_order_relaxed) + 1;
  // Diagnostic (round 9): name the holder and its registry state so a
  // timeout postmortem can tell "live holder stuck elsewhere" from "intent
  // owned by a transaction that already left the registry" (a leak), plus
  // the current wait-for graph (every intent wait in flight). One line per
  // timeout is proportionate: each timeout is itself a multi-second stall.
  // Caller holds the row's shard mutex: read the pending owner without
  // re-locking (std::mutex is not recursive; re-locking here deadlocked a
  // benchmark worker inside the very wait loop this hook reports).
  std::string holder_state = "no-pending";
  {
    VersionShard& shard = version_shards_[VersionShardIndex(rp)];
    const auto found = shard.versions.find(rp);
    if (found != shard.versions.end()) {
      std::optional<PendingVersion>& pending = found->second.pending;
      if (pending.has_value()) {
        const txn_id_t holder = pending->owner;
        holder_state = "holder=" + std::to_string(holder);
        std::scoped_lock registry_lock(transaction_table_lock);
        const auto active = active_transactions_.find(holder);
        if (active == active_transactions_.end()) {
          holder_state += " NOT-IN-REGISTRY";
        } else if (active->second != nullptr) {
          holder_state += std::string(" status=") +
                          (active->second->IsAbortStarted() ? "aborting"
                           : active->second->IsFinished()   ? "finished"
                                                            : "running");
          if (const char* phase = active->second->debug_phase_.load(
                  std::memory_order_relaxed)) {
            holder_state += std::string(" phase=") + phase;
          }
          holder_state +=
              " tid=" + std::to_string(active->second->debug_tid_.load(
                            std::memory_order_relaxed));
        }
      }
    }
  }
  {
    std::scoped_lock graph_lock(wait_for_mu_);
    holder_state += " waits-for{";
    for (const auto& [waiter, edge] : wait_for_edges_) {
      holder_state +=
          std::to_string(waiter) + "->" + std::to_string(edge.holder) + " ";
    }
    holder_state += "}";
  }
  holder_state += " " + DebugDumpAllIndexReservations(txn.ID());
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t last = last_intent_timeout_warn_ms_.load(std::memory_order_relaxed);
  if (now_ms - last < kIntentTimeoutWarnInterval.count() ||
      !last_intent_timeout_warn_ms_.compare_exchange_weak(
          last, now_ms, std::memory_order_relaxed)) {
    LOG(WARN) << "write-intent wait timed out after "
              << GetWriteIntentWaitLimit().count() << " ms: txn " << txn.ID()
              << " gave up on row {" << rp.page_id << "," << rp.slot
              << "} (rate-limited; total timeouts: " << timeouts << ") "
              << holder_state;
    return;
  }
  LOG(WARN) << "write-intent wait timed out after "
            << GetWriteIntentWaitLimit().count() << " ms: txn " << txn.ID()
            << " gave up on row {" << rp.page_id << "," << rp.slot
            << "} (the holder could not make progress; usually a page-latch "
               "cycle only an abort can break). total timeouts: "
            << timeouts << " " << holder_state;
}

bool TransactionManager::SnapshotSeesRow(const Transaction& txn,
                                         const RowPosition& rp) const {
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    return false;
  }
  const VersionChain& chain = found->second;
  // An own staged write already supersedes whatever the snapshot would
  // see at this position (e.g. a delete staged by this transaction makes
  // reusing the slot an in-place replace, not a masked version).  An
  // unstaged pending is the intent RowPage::Insert just reserved while
  // probing the hole; it must not suppress the committed-history check.
  if (chain.pending && chain.pending->owner == txn.ID() &&
      chain.pending->staged) {
    return false;
  }
  for (const auto& version : std::ranges::reverse_view(chain.committed)) {
    if (version.begin_ts <= txn.SnapshotTimestamp() &&
        txn.SnapshotTimestamp() < version.end_ts) {
      return version.value.has_value();
    }
  }
  return false;
}

bool TransactionManager::ReleaseWriteIntent(Transaction& txn,
                                            const RowPosition& rp) {
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    return true;
  }
  VersionChain& chain = found->second;
  if (!chain.pending || chain.pending->owner != txn.ID() ||
      chain.pending->staged) {
    return false;
  }
  chain.pending.reset();
  shard.write_intent_released.notify_all();
  if (chain.committed.empty()) {
    ForgetVersionChain(rp);
    shard.versions.erase(found);
  }
  return true;
}

std::string TransactionManager::DebugDumpVersionChains(page_id_t pid) const {
  std::string out;
  for (const VersionShard& shard : version_shards_) {
    std::scoped_lock lock(shard.mutex);
    for (const auto& [rp, chain] : shard.versions) {
      if (rp.page_id != pid) {
        continue;
      }
      out += "    chain {" + std::to_string(rp.page_id) + "," +
             std::to_string(rp.slot) + "} committed=[";
      for (const CommittedVersion& v : chain.committed) {
        out += "{" + std::to_string(v.begin_ts) + "," +
               std::to_string(v.end_ts) + "," +
               (v.value ? "row:" + std::to_string(v.value->size()) + "B"
                        : "null") +
               "}";
      }
      out += "]";
      if (chain.pending) {
        out += " pending=txn" + std::to_string(chain.pending->owner) +
               (chain.pending->staged ? "/staged" : "/unstaged") +
               (chain.pending->value ? "/row" : "/null");
      }
      out += "\n";
    }
  }
  return out;
}

TransactionRuntimeStats TransactionManager::RuntimeStats() const {
  uint64_t chains = 0;
  for (const auto& count : page_chain_counts_) {
    chains += static_cast<uint64_t>(count.load(std::memory_order_relaxed));
  }
  size_t active = 0;
  uint64_t oldest_snapshot = 0;
  {
    std::scoped_lock lk(transaction_table_lock);
    active = active_transactions_.size();
    for (const auto& [id, snapshot] : active_snapshots_) {
      if (oldest_snapshot == 0 || snapshot < oldest_snapshot) {
        oldest_snapshot = snapshot;
      }
    }
  }
  const auto sync = logger_->GetSyncStats();
  return {
      .wal_sync_count = sync.count,
      .wal_sync_ns = sync.nanoseconds,
      .wal_buffered_bytes = logger_->BufferedLSN(),
      .wal_durable_bytes = logger_->DurableLSN(),
      .wal_wait_count = wal_wait_count_.load(std::memory_order_relaxed),
      .wal_wait_ns = wal_wait_ns_.load(std::memory_order_relaxed),
      .write_intent_attempts =
          write_intent_attempts_.load(std::memory_order_relaxed),
      .write_intent_conflicts =
          write_intent_conflicts_.load(std::memory_order_relaxed),
      .write_intent_wait_timeouts =
          write_intent_wait_timeouts_.load(std::memory_order_relaxed),
      .write_intent_holder_abort_giveups =
          write_intent_holder_abort_giveups_.load(std::memory_order_relaxed),
      .write_intent_mutex_wait_ns =
          write_intent_mutex_wait_ns_.load(std::memory_order_relaxed),
      .commit_shard_mutex_wait_ns =
          commit_shard_mutex_wait_ns_.load(std::memory_order_relaxed),
      .active_transactions = static_cast<uint64_t>(active),
      .version_chains = chains,
      .oldest_snapshot_ts = oldest_snapshot,
      .current_commit_ts = commit_timestamp_.load(std::memory_order_relaxed),
      .index_undo_reserved_pages =
          [&] {
            size_t pages = 0;
            for (const IndexReservationShard& shard : index_reservations_) {
              std::scoped_lock lock(shard.mutex);
              pages += shard.bytes.size();
            }
            return pages;
          }(),
      .index_gate_contended_acquires =
          index_gate_contended_acquires_.load(std::memory_order_relaxed),
      .index_gate_wait_ns = index_gate_wait_ns_.load(std::memory_order_relaxed),
      .index_gate_long_waits =
          index_gate_long_waits_.load(std::memory_order_relaxed),
      .table_tail_wait_count =
          page_manager_ == nullptr ? 0 : page_manager_->TailWaitCount(),
      .table_tail_wait_ns =
          page_manager_ == nullptr ? 0 : page_manager_->TailWaitNs(),
      .index_only_visible_serves =
          index_only_visible_serves_.load(std::memory_order_relaxed),
      .index_scan_prefetch_pages =
          index_scan_prefetch_pages_.load(std::memory_order_relaxed),
  };
}

StatusOr<std::optional<std::string>> TransactionManager::ReadVersion(
    const Transaction& txn, const RowPosition& rp,
    std::optional<std::string_view> physical, bool resolve_head) const {
  if (fail_stop_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  // Fused chain-free path: the zero shard counter proves no chain exists for
  // this row (Transaction::ReadVersion normally routes these here only for
  // direct callers; either way one atomic load replaces the shard lock).
  if (physical && page_chain_counts_[PageChainShardIndex(rp.page_id)].load(
                      std::memory_order_acquire) == 0) {
    return std::optional<std::string>();
  }
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    if (!physical) {
      return Status::kNotExists;
    }
    // nullopt = the latched page image is the authoritative version; the
    // caller may serve a view of it instead of copying the row body per
    // read (a full scan of chain-bearing pages otherwise pays one string
    // allocation + copy per row for a value equal to the image it holds).
    return std::optional<std::string>();
  }
  const VersionChain& chain = found->second;
  if (chain.pending && chain.pending->owner == txn.ID() &&
      chain.pending->staged) {
    if (!chain.pending->value) {
      return Status::kNotExists;
    }
    return *chain.pending->value;
  }
  // An empty committed list means the chain carries no snapshot-relevant
  // version: either GC collected a chain made redundant by the heap image
  // (leaving at most an unstaged intent shell), or the intent was acquired
  // without a before-image on a row whose chain no longer exists. In both
  // cases the physical image is authoritative for every snapshot that could
  // reach this branch -- any newer value is still uncommitted and any older
  // value was already proven equal to the physical image when GC dropped
  // the chain. Without this fallback the row reads back as kNotExists and
  // silently disappears from scans after a commit+GC+re-intent cycle.
  // (A bare INSERT intent also produces an empty committed list; updates
  // keep their before-image in `committed` via AcquireWriteIntent so this
  // fallback is never what serves a stale post-update heap image.)
  if (chain.committed.empty()) {
    if (!physical) {
      return Status::kNotExists;
    }
    return std::optional<std::string>();
  }
  // A writer that waited for a predecessor implements strict write locking,
  // not first-updater-wins snapshot isolation: its SET expression must see
  // the predecessor's committed result.  The newest committed entry is kept
  // while this unstaged intent exists.  When GC removed a redundant chain
  // before the intent was acquired, the heap is the authoritative latest
  // image and the physical fallback below is equivalent.
  if (resolve_head && chain.pending && chain.pending->owner == txn.ID()) {
    if (!chain.committed.empty()) {
      const CommittedVersion& latest = chain.committed.back();
      // D4 (docs/design.md): a writer reading a predecessor's committed
      // result depends on that commit's durability.
      if (latest.commit_lsn != 0) {
        txn.RecordDurabilityDependence(latest.commit_lsn);
      }
      if (!latest.value) {
        return Status::kNotExists;
      }
      return *latest.value;
    }
    if (physical) {
      return std::optional<std::string>();
    }
  }
  for (const auto& version : std::ranges::reverse_view(chain.committed)) {
    if (version.begin_ts <= txn.SnapshotTimestamp() &&
        txn.SnapshotTimestamp() < version.end_ts) {
      // D4 (docs/design.md): record the commit that made this version
      // visible as a durability dependency; PreCommit waits for it before
      // the result reaches the user (read-only included).
      if (version.commit_lsn != 0) {
        txn.RecordDurabilityDependence(version.commit_lsn);
      }
      if (!version.value) {
        return Status::kNotExists;
      }
      return *version.value;
    }
  }
  return Status::kNotExists;
}

StatusOr<std::optional<std::string>> TransactionManager::ReadLatestVersion(
    const Transaction& txn, const RowPosition& rp,
    std::optional<std::string_view> physical) const {
  if (fail_stop_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  if (physical && page_chain_counts_[PageChainShardIndex(rp.page_id)].load(
                      std::memory_order_acquire) == 0) {
    return std::optional<std::string>();
  }
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    if (!physical) {
      return Status::kNotExists;
    }
    return std::optional<std::string>();
  }
  const VersionChain& chain = found->second;
  if (chain.pending && chain.pending->owner == txn.ID() &&
      chain.pending->staged) {
    if (!chain.pending->value) {
      return Status::kNotExists;
    }
    return *chain.pending->value;
  }
  if (!chain.committed.empty()) {
    const CommittedVersion& latest = chain.committed.back();
    if (latest.commit_lsn != 0) {
      txn.RecordDurabilityDependence(latest.commit_lsn);
    }
    if (!latest.value) {
      return Status::kNotExists;
    }
    return *latest.value;
  }
  if (!physical) {
    return Status::kNotExists;
  }
  return std::optional<std::string>();
}

bool TransactionManager::HasVersionChain(const RowPosition& rp) const {
  // Read fast path: page_chain_counts_ is maintained at every chain
  // create/erase under the same shard mutex, so a zero count proves this row
  // has no version chain without the shard mutex + map probe. Read-mostly
  // scans pay one acquire load; a nonzero count (or a count racing a
  // concurrent create/erase) falls back to the authoritative shard map.
  // Deliberately NO per-page-map shortcut here: rows of a chain-bearing
  // page would pay an extra mutex acquisition per read on top of the
  // authoritative probe below. The per-page answer is asked once per page
  // by the scan's PhysicalReadEligible (HasPageVersionChains), which is
  // where the exact map pays off.
  if (page_chain_counts_[PageChainShardIndex(rp.page_id)].load(
          std::memory_order_acquire) == 0) {
    return false;
  }
  const VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  return shard.versions.contains(rp);
}

bool TransactionManager::HasPageVersionChains(page_id_t pid) const {
  const size_t chain_shard_index = PageChainShardIndex(pid);
  if (page_chain_counts_[chain_shard_index].load(std::memory_order_acquire) ==
      0) {
    return false;
  }
  const PageChainShard& shard = page_chain_shards_[chain_shard_index];
  std::scoped_lock lock(shard.mutex);
  return shard.counts.contains(pid);
}

void TransactionManager::InvalidatePageVersions(page_id_t pid,
                                                Transaction* reclaimer) {
  for (VersionShard& shard : version_shards_) {
    std::scoped_lock lock(shard.mutex);
    bool erased = false;
    for (auto it = shard.versions.begin(); it != shard.versions.end();) {
      if (it->first.page_id == pid) {
        ForgetVersionChain(it->first);
        it = shard.versions.erase(it);
        erased = true;
      } else {
        ++it;
      }
    }
    // A waiter parked on a recycled page's write intent must re-evaluate:
    // its chain is gone, so acquisition now succeeds.
    if (erased) {
      shard.write_intent_released.notify_all();
    }
  }
  {
    std::scoped_lock registry_lock(transaction_table_lock);
    for (const auto& [id, txn] : active_transactions_) {
      if (txn != nullptr) {
        txn->InvalidateVersionReadCache();
      }
    }
  }
  if (reclaimer != nullptr) {
    // The reclaimer may hold write intents acquired against the dead
    // incarnation (e.g. INSERT ... DROP ... CREATE in one transaction).
    // Erasing the chains above removed the reservations these entries
    // describe; leaving them in the write set would let a later write on
    // the new incarnation skip AcquireWriteIntent and reach
    // RegisterVersionWrite with no pending intent.
    reclaimer->write_set_.erase_if(
        [pid](const RowPosition& rp) { return rp.page_id == pid; });
  }
}

void TransactionManager::CommitRecycledPageAllocation(Transaction& txn,
                                                      page_id_t pid) {
  for (VersionShard& shard : version_shards_) {
    std::scoped_lock lock(shard.mutex);
    bool erased = false;
    for (auto it = shard.versions.begin(); it != shard.versions.end();) {
      if (it->first.page_id != pid) {
        ++it;
        continue;
      }
      if (it->second.pending && it->second.pending->owner == txn.ID()) {
        it->second.committed.clear();
        ++it;
        continue;
      }
      ForgetVersionChain(it->first);
      it = shard.versions.erase(it);
      erased = true;
    }
    if (erased) {
      shard.write_intent_released.notify_all();
    }
  }
  std::scoped_lock registry_lock(transaction_table_lock);
  for (const auto& [id, active] : active_transactions_) {
    if (active != nullptr) {
      active->InvalidateVersionReadCache();
    }
  }
}

bool TransactionManager::IsActive(txn_id_t id) const {
  std::scoped_lock lk(transaction_table_lock);
  return active_transactions_.contains(id);
}

bool TransactionManager::IsAborting(txn_id_t id) const {
  if (id == 0) {
    return false;
  }
  // The registry lookup under the table lock pins the find: a transaction
  // cannot finish (and be erased) between the find and the flag read, so
  // dereferencing the pointer here is safe without any other coordination.
  std::scoped_lock lk(transaction_table_lock);
  const auto found = active_transactions_.find(id);
  return found != active_transactions_.end() && found->second != nullptr &&
         found->second->IsAbortStarted();
}

void TransactionManager::RegisterVersionWrite(
    Transaction& txn, const RowPosition& rp,
    std::optional<std::string_view> before,
    std::optional<std::string_view> after) {
  (void)txn;
  std::optional<std::string> before_copy =
      before ? std::optional<std::string>(std::string(*before)) : std::nullopt;
  std::optional<std::string> after_copy =
      after ? std::optional<std::string>(std::string(*after)) : std::nullopt;
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  VersionChain& chain = GetOrCreateVersionChain(shard, rp);
  if (chain.committed.empty()) {
    chain.committed.push_back(
        {0, std::numeric_limits<uint64_t>::max(), std::move(before_copy)});
  }
  // AddWriteSet reserves the pending slot before the physical image changes.
  // Reaching this function without that reservation is an invariant breach.
  // assert() alone vanishes in NDEBUG builds, silently dropping the write;
  // keep a loud log on every build so the breach is diagnosable.
  if (!chain.pending.has_value()) {
    LOG(ERROR) << "RegisterVersionWrite without reserved write intent";
    assert(false);
    return;
  }
  if (chain.pending->owner != txn.ID()) {
    LOG(ERROR) << "RegisterVersionWrite intent owner mismatch";
    assert(false);
    return;
  }
  chain.pending->value = std::move(after_copy);
  chain.pending->staged = true;
}

void TransactionManager::MarkPendingIndexMutation(page_id_t index_root) {
  const size_t shard = IndexMutationShardIndex(index_root);
  pending_index_mutations_[shard].fetch_add(1, std::memory_order_release);
  pending_index_mutation_count_.fetch_add(1, std::memory_order_release);
}

void TransactionManager::TrackIndexKeyMutation(txn_id_t owner,
                                               page_id_t index_root,
                                               std::string_view key) {
  if (!index_key_tracking_complete_.load(std::memory_order_acquire)) {
    return;
  }
  IndexKeyHistory& history =
      index_key_history_[IndexMutationShardIndex(index_root)];
  if (!history.precise) {
    return;
  }
  try {
    constexpr size_t kMaxPendingOwners = 128;
    constexpr size_t kMaxPendingKeys = 512;
    constexpr size_t kMaxPendingBytes = size_t{64} * 1024;
    auto pending =
        std::find_if(history.pending.begin(), history.pending.end(),
                     [&](const PendingIndexKeys& p) {
                       return p.owner == owner && p.root == index_root;
                     });
    if (pending != history.pending.end() &&
        std::find(pending->keys.begin(), pending->keys.end(), key) !=
            pending->keys.end()) {
      return;
    }
    if (key.empty() || key.size() > kMaxPendingBytes - history.pending_bytes ||
        history.pending_keys >= kMaxPendingKeys ||
        (pending == history.pending.end() &&
         history.pending.size() >= kMaxPendingOwners)) {
      history.Disable();
      return;
    }
    if (pending == history.pending.end()) {
      history.pending.push_back({owner, index_root, {}});
      pending = std::prev(history.pending.end());
    }
    pending->keys.emplace_back(key);
    history.pending_bytes += key.size();
    ++history.pending_keys;
  } catch (const std::bad_alloc&) {
    history.Disable();
  }
}

void TransactionManager::FinishIndexKeyChanges(
    txn_id_t owner, page_id_t index_root, std::optional<uint64_t> commit_ts) {
  IndexKeyHistory& history =
      index_key_history_[IndexMutationShardIndex(index_root)];
  if (!history.precise) {
    return;
  }
  try {
    auto pending =
        std::find_if(history.pending.begin(), history.pending.end(),
                     [&](const PendingIndexKeys& p) {
                       return p.owner == owner && p.root == index_root;
                     });
    if (pending == history.pending.end()) {
      // Unknown-key callers already revoke manager-wide precision. A missing
      // registration from any other path must also remain conservative.
      history.Disable();
      return;
    }
    constexpr size_t kMaxHistoryKeys = 512;
    constexpr size_t kMaxHistoryBytes = size_t{64} * 1024;
    for (std::string& key : pending->keys) {
      history.pending_bytes -= key.size();
      --history.pending_keys;
      if (commit_ts) {
        history.history_bytes += key.size();
        history.committed.push_back({index_root, *commit_ts, std::move(key)});
        while (history.committed.size() > kMaxHistoryKeys ||
               history.history_bytes > kMaxHistoryBytes) {
          const auto& old = history.committed.front();
          history.forgotten_through_ts =
              std::max(history.forgotten_through_ts, old.commit_ts);
          history.history_bytes -= old.key.size();
          history.committed.pop_front();
        }
      }
    }
    history.pending.erase(pending);
  } catch (const std::bad_alloc&) {
    // Version publication may already have released write intents. Optional
    // precision must never turn that irreversible commit into a failure.
    history.Disable();
  }
}

std::optional<std::vector<std::string>>
TransactionManager::IndexKeyChangesSinceSnapshot(
    const Transaction& txn, page_id_t index_root,
    const IndexReadGuard& guard) const {
  const size_t shard = IndexMutationShardIndex(index_root);
  if (!guard.HoldsLock() || guard.lock_.mutex() != &index_gates_[shard] ||
      !index_key_tracking_complete_.load(std::memory_order_acquire)) {
    return std::nullopt;
  }
  const IndexKeyHistory& history = index_key_history_[shard];
  if (!history.precise ||
      txn.SnapshotTimestamp() < history.forgotten_through_ts ||
      pending_index_mutations_[shard].load(std::memory_order_acquire) !=
          history.pending.size()) {
    return std::nullopt;
  }
  try {
    std::vector<std::string> keys;
    for (const auto& pending : history.pending) {
      if (pending.root == index_root) {
        keys.insert(keys.end(), pending.keys.begin(), pending.keys.end());
      }
    }
    for (const auto& committed : history.committed) {
      if (committed.root == index_root &&
          txn.SnapshotTimestamp() < committed.commit_ts) {
        keys.push_back(committed.key);
      }
    }
    return keys;
  } catch (const std::bad_alloc&) {
    return std::nullopt;
  }
}

void TransactionManager::ReleasePendingIndexMutation(size_t shard) {
  std::atomic<uint64_t>& pending = pending_index_mutations_[shard];
  uint64_t current = pending.load(std::memory_order_acquire);
  while (current != 0 && !pending.compare_exchange_weak(
                             current, current - 1, std::memory_order_acq_rel,
                             std::memory_order_acquire)) {
  }
  if (current != 0) {
    pending_index_mutation_count_.fetch_sub(1, std::memory_order_acq_rel);
  }
}

void TransactionManager::CommitIndexMutations(Transaction& txn,
                                              uint64_t commit_ts) {
  for (const page_id_t root : txn.mutated_index_roots_) {
    const size_t shard = IndexMutationShardIndex(root);
    std::unique_lock<std::shared_mutex> lock(index_gates_[shard]);
    FinishIndexKeyChanges(txn.ID(), root, commit_ts);
    std::atomic<uint64_t>& latest = max_index_mutation_ts_[shard];
    uint64_t observed = latest.load(std::memory_order_relaxed);
    while (observed < commit_ts &&
           !latest.compare_exchange_weak(observed, commit_ts,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
    }
    ReleasePendingIndexMutation(shard);
  }
  txn.mutated_index_roots_.clear();
}

void TransactionManager::ClearPendingIndexMutations(Transaction& txn) {
  for (const page_id_t root : txn.mutated_index_roots_) {
    const size_t shard = IndexMutationShardIndex(root);
    std::unique_lock<std::shared_mutex> lock(index_gates_[shard]);
    FinishIndexKeyChanges(txn.ID(), root, std::nullopt);
    ReleasePendingIndexMutation(shard);
  }
  txn.mutated_index_roots_.clear();
}

bool TransactionManager::IndexKeysMayBeStale(const Transaction& txn) const {
  return pending_index_mutation_count_.load(std::memory_order_acquire) != 0 ||
         txn.SnapshotTimestamp() <
             max_committed_begin_ts_.load(std::memory_order_acquire);
}

bool TransactionManager::IndexKeysMayBeStale(const Transaction& txn,
                                             page_id_t index_root) const {
  const size_t shard = IndexMutationShardIndex(index_root);
  return pending_index_mutations_[shard].load(std::memory_order_acquire) != 0 ||
         txn.SnapshotTimestamp() <
             max_index_mutation_ts_[shard].load(std::memory_order_acquire);
}

bool TransactionManager::RequiresHistoricalRead(const Transaction& txn) const {
  return IndexKeysMayBeStale(txn);
}

namespace {
thread_local std::vector<std::pair<const std::shared_mutex*, uint32_t>>
    tls_held_index_read_gates;

uint32_t* FindTlsGateCount(const std::shared_mutex* m) {
  for (auto& entry : tls_held_index_read_gates) {
    if (entry.first == m) {
      return &entry.second;
    }
  }
  return nullptr;
}
}  // namespace

TransactionManager::IndexReadGuard::IndexReadGuard(std::shared_mutex& mutex)
    : tracked_mutex_(&mutex) {
  if (uint32_t* count = FindTlsGateCount(&mutex);
      count != nullptr && *count > 0) {
    ++(*count);
    borrowed_ = true;
    return;
  }
  lock_ = std::shared_lock<std::shared_mutex>(mutex);
  if (uint32_t* count = FindTlsGateCount(&mutex)) {
    *count = 1;
  } else {
    tls_held_index_read_gates.emplace_back(&mutex, 1U);
  }
}

TransactionManager::IndexReadGuard::IndexReadGuard(
    IndexReadGuard&& other) noexcept
    : lock_(std::move(other.lock_)),
      tracked_mutex_(std::exchange(other.tracked_mutex_, nullptr)),
      borrowed_(std::exchange(other.borrowed_, false)) {}

TransactionManager::IndexReadGuard&
TransactionManager::IndexReadGuard::operator=(IndexReadGuard&& other) noexcept {
  if (this != &other) {
    if (tracked_mutex_ != nullptr) {
      if (uint32_t* count = FindTlsGateCount(tracked_mutex_);
          count != nullptr && *count > 0) {
        --(*count);
      }
    }
    if (lock_.owns_lock()) {
      lock_.unlock();
    }
    lock_ = std::move(other.lock_);
    tracked_mutex_ = std::exchange(other.tracked_mutex_, nullptr);
    borrowed_ = std::exchange(other.borrowed_, false);
  }
  return *this;
}

TransactionManager::IndexReadGuard::~IndexReadGuard() {
  if (tracked_mutex_ != nullptr) {
    if (uint32_t* count = FindTlsGateCount(tracked_mutex_);
        count != nullptr && *count > 0) {
      --(*count);
    }
  }
}

TransactionManager::IndexReadGuard TransactionManager::AcquireIndexReadGuard(
    page_id_t index_root) const {
  return IndexReadGuard(index_gates_[IndexMutationShardIndex(index_root)]);
}

std::unique_lock<std::shared_mutex> TransactionManager::AcquireIndexWriteGuard(
    page_id_t index_root) const {
  auto& gate = index_gates_[IndexMutationShardIndex(index_root)];
  // Sampled contention accounting: the uncontended RMW majority must not pay
  // for atomics, so only acquisitions that block >1ms record anything.
  const auto wait_started = std::chrono::steady_clock::now();
  std::unique_lock<std::shared_mutex> lock(gate);
  const auto waited = std::chrono::steady_clock::now() - wait_started;
  const auto wait_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(waited).count();
  if (wait_ns > 1'000'000) {
    index_gate_contended_acquires_.fetch_add(1, std::memory_order_relaxed);
    index_gate_wait_ns_.fetch_add(static_cast<uint64_t>(wait_ns),
                                  std::memory_order_relaxed);
    if (wait_ns > 1'000'000'000) {
      index_gate_long_waits_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  return lock;
}

std::vector<std::unique_lock<std::shared_mutex>>
TransactionManager::AcquireIndexWriteGuards(
    const std::vector<page_id_t>& index_roots) const {
  std::vector<size_t> shards;
  shards.reserve(index_roots.size());
  for (const page_id_t root : index_roots) {
    shards.push_back(IndexMutationShardIndex(root));
  }
  std::ranges::sort(shards);
  shards.erase(std::unique(shards.begin(), shards.end()), shards.end());
  std::vector<std::unique_lock<std::shared_mutex>> locks;
  locks.reserve(shards.size());
  for (const size_t shard : shards) {
    locks.emplace_back(index_gates_[shard]);
  }
  return locks;
}

uint64_t TransactionManager::AllocatePendingCommit() {
  // Allocation and registration share pending_commits_mutex_: a timestamp
  // must be in unpublished_commits_ before any other thread can observe it
  // through commit_timestamp_, otherwise PublishCommit's empty-set branch
  // could advance stable_timestamp_ past an in-flight commit and a new
  // snapshot would later see that commit despite starting before it.
  std::scoped_lock lk(pending_commits_mutex_);
  const uint64_t ts = commit_timestamp_.fetch_add(1) + 1;
  unpublished_commits_.insert(ts);
  return ts;
}

void TransactionManager::PublishCommit(uint64_t ts) {
  uint64_t next_stable = ts;
  {
    std::scoped_lock lk(pending_commits_mutex_);
    unpublished_commits_.erase(ts);
    // Only advance to the smallest still-unpublished timestamp - 1: a
    // snapshot at or below it is guaranteed to see every version with
    // begin_ts <= it, because those publications completed before this
    // store (release) and Begin() reads stable_timestamp_ with acquire.
    next_stable = unpublished_commits_.empty()
                      ? commit_timestamp_.load(std::memory_order_relaxed)
                      : *unpublished_commits_.begin() - 1;
  }
  // Monotonic max, not a read-then-store: publishers run concurrently, and a
  // thread holding a smaller next_stable that paused after its load must not
  // overwrite a larger value another publisher already installed. A regressing
  // stable timestamp hands a fresh snapshot an older view than an already
  // acknowledged commit, which the GC may have collected in the meantime.
  uint64_t current = stable_timestamp_.load(std::memory_order_relaxed);
  while (current < next_stable) {
    if (stable_timestamp_.compare_exchange_weak(current, next_stable,
                                                std::memory_order_release,
                                                std::memory_order_relaxed)) {
      break;
    }
  }
}

void TransactionManager::CommitVersions(Transaction& txn, lsn_t commit_lsn) {
  // Publish row versions one write-set entry at a time under that entry's
  // shard lock alone. Readers only ever inspect ONE chain under ONE shard
  // lock, so per-chain atomicity is all the protocol needs; the commit
  // becomes VISIBLE atomically later, when PublishCommit advances
  // stable_timestamp_ past commit_ts -- until then no snapshot can match a
  // version stamped begin_ts == commit_ts. (The previous draft held every
  // touched shard lock simultaneously: a manager-wide lock convoy on the
  // commit path, buying atomicity the timestamp protocol already provides.)
  // A bad_alloc inside one entry's publication leaves that entry unfinished
  // -- the pending intent is still ours -- and the loop retries the
  // unfinished suffix; earlier entries are done and never revisited. The
  // commit_ts therefore either fully publishes or stays registered as
  // unpublished (snapshots stall conservatively), never half-exposes.
  auto position = txn.write_set_.begin();
  const auto end = txn.write_set_.end();
  const size_t entry_count = txn.write_set_.size();
  const uint64_t commit_ts = AllocatePendingCommit();
  size_t next = 0;
  bool retried_bad_alloc = false;
  while (position != end) {
    try {
      const RowPosition& rp = *position;
      VersionShard& shard = version_shards_[VersionShardIndex(rp)];
      const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
      const auto wait_start = measure ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
      std::scoped_lock lock(shard.mutex);
      if (measure) {
        commit_shard_mutex_wait_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - wait_start)
                    .count()),
            std::memory_order_relaxed);
      }
      const auto found = shard.versions.find(rp);
      if (found == shard.versions.end()) {
        ++position;
        ++next;
        continue;
      }
      VersionChain& chain = found->second;
      if (!chain.pending || chain.pending->owner != txn.ID()) {
        ++position;
        ++next;
        continue;
      }
      if (!chain.pending->staged) {
        chain.pending.reset();
        shard.write_intent_released.notify_all();
        ++position;
        ++next;
        continue;
      }
      if (!chain.committed.empty()) {
        chain.committed.back().end_ts = commit_ts;
      }
      // Copy (not move) the staged value: a push_back failure inside the
      // bad_alloc retry loop must leave the pending value intact for the
      // retried attempt -- a moved-out optional would publish an empty
      // string on the retry. The pending reset below discards the copy
      // source on success.
      chain.committed.push_back({commit_ts,
                                 std::numeric_limits<uint64_t>::max(),
                                 chain.pending->value, commit_lsn});
      chain.pending.reset();
      shard.write_intent_released.notify_all();
      ++position;
      ++next;
    } catch (const std::bad_alloc&) {
      if (retried_bad_alloc) {
        throw;
      }
      retried_bad_alloc = true;
      // Transient allocation failure mid-publication: retry the suffix. The
      // partial state is safe -- every completed entry is a valid committed
      // version invisible to snapshots until PublishCommit below.
      LOG(ERROR) << "commit publication hit bad_alloc at entry " << next
                 << " of " << entry_count << ", retrying";
    }
  }
  uint64_t observed_commit_ts =
      max_committed_begin_ts_.load(std::memory_order_relaxed);
  while (observed_commit_ts < commit_ts &&
         !max_committed_begin_ts_.compare_exchange_weak(
             observed_commit_ts, commit_ts, std::memory_order_release,
             std::memory_order_relaxed)) {
  }
  CommitIndexMutations(txn, commit_ts);
  // From here on every snapshot >= commit_ts observes this publication.  If
  // the (allocation-free in practice) publication above ever throws,
  // commit_ts stays registered as unpublished: correct (snapshots stall at
  // the previous stable point), just conservatively slow.
  PublishCommit(commit_ts);
}

void TransactionManager::AbortVersions(Transaction& txn) {
  for (const RowPosition& rp : txn.write_set_) {
    VersionShard& shard = version_shards_[VersionShardIndex(rp)];
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.versions.find(rp);
    if (found != shard.versions.end() && found->second.pending &&
        found->second.pending->owner == txn.ID()) {
      found->second.pending.reset();
      shard.write_intent_released.notify_all();
    }
  }
  ClearPendingIndexMutations(txn);
}

void TransactionManager::MoveActiveTransaction(Transaction* from,
                                               Transaction* to) {
  std::scoped_lock lk(transaction_table_lock);
  if (active_transactions_.empty()) {
    return;
  }
  auto it = active_transactions_.find(from->txn_id_);
  if (it != active_transactions_.end() && it->second == from) {
    it->second = to;
    if (from->IsWounded()) {
      to->Wound();
    }
  }
}

void TransactionManager::UnregisterActiveTransaction(Transaction* txn) {
  std::scoped_lock lk(transaction_table_lock);
  if (active_transactions_.empty()) {
    return;
  }
  auto it = active_transactions_.find(txn->txn_id_);
  if (it != active_transactions_.end() && it->second == txn) {
    active_transactions_.erase(it);
    // The move-assignment adopts a new identity next; a snapshot entry left
    // under the old one would pin GarbageCollectVersions forever (the
    // destructor only releases the entry keyed by the adopted id).
    active_snapshots_.erase(txn->txn_id_);
  }
}

void TransactionManager::ReleaseActiveTransaction(Transaction* txn) {
  std::scoped_lock lk(transaction_table_lock);
  // Early-out covers transactions whose manager was detached by our
  // destructor path as well as finished ones; there is nothing to release.
  if (active_transactions_.empty()) {
    return;
  }
  auto it = active_transactions_.find(txn->txn_id_);
  if (it != active_transactions_.end() && it->second == txn) {
    active_transactions_.erase(it);
    active_snapshots_.erase(txn->txn_id_);
    if (!txn->IsReadOnly()) {
      const uint64_t pending =
          commits_since_gc_.fetch_add(1, std::memory_order_relaxed) + 1;
      if (pending == kGcCommitThreshold) {
        std::scoped_lock wake(gc_mutex_);
        gc_cv_.notify_all();
      }
    }
  }
}

int TransactionManager::ReservationBisectMode() {
  // Diagnostic bisect for the reservation machinery (env
  // TINYLAMB_DISABLE_INDEX_RESERVATION): 0=on, 1=off, 2=no shared map,
  // 3=no per-transaction vector, 4=registration on but admission discount
  // off, 5=discount on but the tree-level drain retry off.
  static const int mode = [] {
    const char* env = std::getenv("TINYLAMB_DISABLE_INDEX_RESERVATION");
    if (env == nullptr) {
      return 0;
    }
    char* parsed_end = nullptr;
    const long parsed = std::strtol(env, &parsed_end, 10);
    return parsed_end == env || *parsed_end != '\0' ? 0
                                                    : static_cast<int>(parsed);
  }();
  return mode;
}

void TransactionManager::ReserveIndexUndoBytes(Transaction& txn, page_id_t pid,
                                               size_t bytes) {
  const int mode = ReservationBisectMode();
  if (mode == 1) {
    return;
  }
  // Record the claim on the transaction first: ForgetTransaction walks this
  // list to release exactly what this transaction reserved, and clearing it
  // there makes a repeated finish a no-op.
  if (mode != 3) {
    txn.index_undo_reservations_.emplace_back(pid, bytes);
  }
  if (mode != 2) {
    IndexReservationShard& shard =
        index_reservations_[IndexReservationShardIndex(pid)];
    std::scoped_lock lock(shard.mutex);
    auto [owner, inserted] = shard.bytes[pid].try_emplace(txn.ID(), 0);
    if (inserted) {
      ++shard.owner_pages[txn.ID()];
    }
    owner->second += bytes;
  }
}

size_t TransactionManager::IndexUndoReservedBytes(page_id_t pid,
                                                  txn_id_t self) const {
  const int bisect = ReservationBisectMode();
  if (bisect != 0 && bisect != 4 && bisect != 5) {
    return 0;
  }
  const IndexReservationShard& shard =
      index_reservations_[IndexReservationShardIndex(pid)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.bytes.find(pid);
  if (found == shard.bytes.end()) {
    return 0;
  }
  size_t total = 0;
  for (const auto& [owner, bytes] : found->second) {
    if (owner != self) {
      total += static_cast<size_t>(bytes);
    }
  }
  return total;
}

Status TransactionManager::WaitForIndexReservationDrain(
    page_id_t pid, txn_id_t self, std::chrono::milliseconds limit) const {
  if (GetDeadlockPolicy() == DeadlockPolicy::kNoWait) {
    limit = std::chrono::milliseconds(0);
  }
  const IndexReservationShard& shard =
      index_reservations_[IndexReservationShardIndex(pid)];
  const auto deadline = std::chrono::steady_clock::now() + limit;
  // |foreign| assumes shard.mutex is already held (the query method re-locks,
  // which a non-recursive mutex forbids).
  const auto foreign = [&]() -> size_t {
    const auto found = shard.bytes.find(pid);
    if (found == shard.bytes.end()) {
      return 0;
    }
    size_t total = 0;
    for (const auto& [owner, bytes] : found->second) {
      if (owner != self) {
        total += static_cast<size_t>(bytes);
      }
    }
    return total;
  };
  std::unique_lock lock(shard.mutex);
  while (foreign() != 0) {
    if (shard.released.wait_until(lock, deadline) == std::cv_status::timeout) {
      return foreign() == 0
                 ? Status::kSuccess
                 : StatusError(StatusCode::kConflicts,
                               "index-undo reservation drain timed out");
    }
  }
  return Status::kSuccess;
}

bool TransactionManager::HasForeignIndexReservations(txn_id_t self) const {
  for (const IndexReservationShard& shard : index_reservations_) {
    std::scoped_lock lock(shard.mutex);
    if (shard.owner_pages.size() > 1 ||
        (!shard.owner_pages.empty() && !shard.owner_pages.contains(self))) {
      return true;
    }
  }
  return false;
}

Status TransactionManager::WaitForAllIndexReservationsDrain(
    txn_id_t self, std::chrono::milliseconds limit) const {
  return WaitForAllIndexReservationsDrain(self, limit, nullptr);
}

Status TransactionManager::WaitForAllIndexReservationsDrain(
    txn_id_t self, std::chrono::milliseconds limit,
    const Transaction* watcher) const {
  // A single-threaded session executor cannot drive reservation owners while
  // parked here. Match write-intent admission's no-wait policy: callers may
  // still attempt an operation that fits, but a required drain must conflict
  // immediately. Undo-byte admission itself remains unchanged.
  if (GetDeadlockPolicy() == DeadlockPolicy::kNoWait) {
    limit = std::chrono::milliseconds(0);
  }
  const auto deadline = std::chrono::steady_clock::now() + limit;
  std::unique_lock drain_lock(reservation_drain_mu_);
  const auto wait_started = std::chrono::steady_clock::now();
  bool diagnosed = false;
  while (HasForeignIndexReservations(self)) {
    // Diagnostic (round 9): a drain that outlives 200 ms is a stall
    // precursor -- every waiter parked here holds its own row intents, so
    // its queue multiplies the damage. Name the foreign owners once per
    // wait so the postmortem can chain holder -> reservation owner. The
    // sliced wait below is what makes this reachable at all: a plain
    // wait_until(deadline) that is never notified only re-evaluates at the
    // deadline, so a multi-second stall produced no diagnostic line.
    if (!diagnosed && std::chrono::steady_clock::now() - wait_started >
                          std::chrono::milliseconds(200)) {
      diagnosed = true;
      LOG(WARN) << "index-undo drain slow: waiter txn " << self << " sees "
                << DebugDumpAllIndexReservations(self);
    }
    if (watcher != nullptr) {
      watcher->DebugSetPhase("reservation-drain");
    }
    // Sliced wait: a wounded transaction must abandon the drain promptly
    // (its abort releases every reservation it holds, which is frequently
    // the event everyone else is waiting for), and the slice re-arms the
    // stall diagnostic even when no release notification ever arrives.
    const auto wake_by = std::min(deadline, std::chrono::steady_clock::now() +
                                                std::chrono::milliseconds(25));
    reservation_drain_cv_.wait_until(drain_lock, wake_by);
    if (watcher != nullptr) {
      watcher->DebugSetPhase(nullptr);
      if (watcher->IsWounded()) {
        return StatusError(StatusCode::kConflicts,
                           "transaction wounded during reservation drain");
      }
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      // Deadline with the foreign reservations still held: give up and let
      // the caller surface its honest kNoSpace. Reservations that drained
      // exactly at the deadline are the success case -- the caller's retry
      // would now fit, so failing it here turns a transient contention
      // window into a statement failure.
      return HasForeignIndexReservations(self)
                 ? StatusError(StatusCode::kConflicts,
                               "index-undo reservation drain timed out")
                 : Status::kSuccess;
    }
  }
  return Status::kSuccess;
}

std::string TransactionManager::DebugDumpIndexReservations(
    page_id_t pid) const {
  const IndexReservationShard& shard =
      index_reservations_[IndexReservationShardIndex(pid)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.bytes.find(pid);
  if (found == shard.bytes.end()) {
    return "(none)";
  }
  std::string out;
  for (const auto& [owner, bytes] : found->second) {
    out += " txn" + std::to_string(owner) + ":" + std::to_string(bytes);
  }
  return out;
}

std::string TransactionManager::DebugDumpAllIndexReservations(
    txn_id_t except) const {
  std::string out;
  size_t foreign_entries = 0;
  for (const IndexReservationShard& shard : index_reservations_) {
    std::scoped_lock lock(shard.mutex);
    for (const auto& [pid, owners] : shard.bytes) {
      for (const auto& [owner, bytes] : owners) {
        if (owner == except) {
          continue;
        }
        if (out.empty()) {
          out = "foreign reservations:";
        }
        ++foreign_entries;
        out += " {page " + std::to_string(pid) + " txn " +
               std::to_string(owner) + " " + std::to_string(bytes) + "B";
        {
          std::scoped_lock registry_lock(transaction_table_lock);
          const auto active = active_transactions_.find(owner);
          if (active == active_transactions_.end()) {
            out += " NOT-IN-REGISTRY";
          } else if (active->second != nullptr) {
            out += std::string(active->second->IsAbortStarted() ? " aborting"
                               : active->second->IsFinished()   ? " finished"
                                                                : " running");
            if (const char* phase = active->second->debug_phase_.load(
                    std::memory_order_relaxed)) {
              out += std::string(" phase=") + phase;
            }
          }
        }
        out += "}";
      }
    }
  }
  return out.empty() ? "(no foreign reservations; cleared during wait)"
                     : out + " entries=" + std::to_string(foreign_entries);
}

void TransactionManager::ReleaseIndexUndoReservations(Transaction& txn) {
  if (txn.index_undo_reservations_.empty()) {
    return;
  }
  for (const auto& [pid, bytes] : txn.index_undo_reservations_) {
    IndexReservationShard& shard =
        index_reservations_[IndexReservationShardIndex(pid)];
    std::scoped_lock lock(shard.mutex);
    const auto page = shard.bytes.find(pid);
    if (page == shard.bytes.end()) {
      continue;
    }
    const auto owner = page->second.find(txn.ID());
    if (owner == page->second.end()) {
      continue;
    }
    if (owner->second > bytes) {
      owner->second -= bytes;
    } else {
      page->second.erase(owner);
      const auto summary = shard.owner_pages.find(txn.ID());
      assert(summary != shard.owner_pages.end() && summary->second > 0);
      if (--summary->second == 0) {
        shard.owner_pages.erase(summary);
      }
      if (page->second.empty()) {
        shard.bytes.erase(page);
      }
    }
    shard.released.notify_all();
  }
  {
    std::scoped_lock drain_lock(reservation_drain_mu_);
  }
  reservation_drain_cv_.notify_all();
  // Clearing here makes the release idempotent: ForgetTransaction can run
  // twice for one transaction (finish path, then destructor), and a second
  // full release would under-count other mutators of the same page.
  txn.index_undo_reservations_.clear();
}

void TransactionManager::ForgetTransaction(Transaction& txn) {
  ReleaseIndexUndoReservations(txn);
  {
    std::scoped_lock lk(transaction_table_lock);
    if (!active_transactions_.empty()) {
      active_transactions_.erase(txn.txn_id_);
    }
    active_snapshots_.erase(txn.txn_id_);
  }
  if (page_manager_ != nullptr) {
    const TransactionStatus status =
        txn.status_.load(std::memory_order_acquire);
    const TableTailResolution resolution =
        status == TransactionStatus::kCommitted
            ? TableTailResolution::kPublish
            : (fail_stop_.load(std::memory_order_acquire)
                   ? TableTailResolution::kPoison
                   : TableTailResolution::kDiscard);
    page_manager_->ResolveTableTailReservations(txn.txn_id_, resolution);
  }
  // GC no longer runs on the commit critical path: the background worker
  // picks the work up (threshold-driven) within a few milliseconds.
  if (!txn.IsReadOnly()) {
    const uint64_t pending =
        commits_since_gc_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (pending == kGcCommitThreshold) {
      // Wake the (notify-driven) GC worker promptly at the threshold.
      {
        std::scoped_lock wake(gc_mutex_);
      }
      gc_cv_.notify_all();
    }
  }
}

void TransactionManager::GarbageCollectVersions() {
  std::optional<uint64_t> oldest_snapshot;
  txn_id_t oldest_snapshot_txn = 0;
  {
    std::scoped_lock lock(transaction_table_lock);
    for (const auto& [id, snapshot] : active_snapshots_) {
      if (!oldest_snapshot || snapshot < *oldest_snapshot) {
        oldest_snapshot = snapshot;
        oldest_snapshot_txn = id;
      }
    }
  }
  const lsn_t durable_lsn = logger_ == nullptr
                                ? std::numeric_limits<lsn_t>::max()
                                : logger_->DurableLSN();

  auto collect_shard = [&](VersionShard& shard) {
    std::scoped_lock lock(shard.mutex);
    for (auto chain_iter = shard.versions.begin();
         chain_iter != shard.versions.end();) {
      VersionChain& chain = chain_iter->second;
      const bool latest_commit_durable =
          chain.committed.empty() || chain.committed.back().commit_lsn == 0 ||
          chain.committed.back().commit_lsn <= durable_lsn;
      // The heap page is the authoritative latest image. Once no active
      // snapshot can need an older image, retaining the latest committed
      // value here is both redundant and disastrous for OLTP: GC would scan
      // every row ever loaded on every pass. A deleted row needs no tombstone
      // for future snapshots either -- its physical slot/index entry is gone.
      if (!chain.pending && latest_commit_durable &&
          (!oldest_snapshot || chain.committed.empty() ||
           chain.committed.back().begin_ts <= *oldest_snapshot)) {
        ForgetVersionChain(chain_iter->first);
        chain_iter = shard.versions.erase(chain_iter);
        continue;
      }
      if (oldest_snapshot) {
        while (chain.committed.size() > 1 &&
               chain.committed[1].begin_ts <= *oldest_snapshot) {
          chain.committed.erase(chain.committed.begin());
        }
      }
      if (!chain.pending && chain.committed.empty()) {
        ForgetVersionChain(chain_iter->first);
        chain_iter = shard.versions.erase(chain_iter);
        continue;
      }
      ++chain_iter;
    }
  };

  // Adaptive pass width: a small version space is swept in one full pass
  // (prompt reclamation at the scale tests and light OLTP run at), while a
  // large one is swept in rotating shard windows -- a full scan of millions
  // of chains every kGcCommitThreshold commits would grow the GC worker's
  // shard-lock interference with commit/read hot paths linearly with
  // database size, forever. The cursor advances by exactly the window size,
  // so consecutive passes tile the shard array without gaps.
  uint64_t total_chains = 0;
  for (const auto& count : page_chain_counts_) {
    total_chains +=
        static_cast<uint64_t>(count.load(std::memory_order_relaxed));
  }
  const size_t shards_this_pass =
      total_chains <= kGcFullScanChainLimit
          ? kVersionShardCount
          : std::max(kVersionShardCount / kGcPassDivisor, size_t{1});
  const size_t begin_shard = gc_cursor_;
  for (size_t i = 0; i < shards_this_pass; ++i) {
    collect_shard(version_shards_[(begin_shard + i) % kVersionShardCount]);
  }
  gc_cursor_ = (begin_shard + shards_this_pass) % kVersionShardCount;
  MaybeWarnGcPin(oldest_snapshot, oldest_snapshot_txn, total_chains);
}

// Rate-limited warning for the one MVCC condition that can silently consume
// memory for months on a long-lived process: a transaction (or a leaked
// snapshot) whose snapshot_ts stays far below the commit frontier pins every
// version newer than it, so chains grow without bound no matter how many GC
// passes run. One warning per interval names the transaction so an operator
// can find and finish it.
void TransactionManager::MaybeWarnGcPin(
    const std::optional<uint64_t>& oldest_snapshot,
    txn_id_t oldest_snapshot_txn, uint64_t total_chains) {
  if (!oldest_snapshot || total_chains <= kGcFullScanChainLimit) {
    return;
  }
  const uint64_t age =
      commit_timestamp_.load(std::memory_order_relaxed) - *oldest_snapshot;
  if (age < kGcPinWarnAgeCommits) {
    return;
  }
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  int64_t last = last_gc_pin_warn_ms_.load(std::memory_order_relaxed);
  if (now_ms - last < kGcPinWarnInterval.count() ||
      !last_gc_pin_warn_ms_.compare_exchange_weak(last, now_ms,
                                                  std::memory_order_relaxed)) {
    return;
  }
  LOG(WARN) << "MVCC version GC is pinned: " << total_chains
            << " chains retained for snapshot ts " << *oldest_snapshot
            << " (transaction " << oldest_snapshot_txn << ", " << age
            << " commits behind the frontier). A long-running or leaked "
               "transaction keeps every intermediate version alive; finish "
               "or roll it back to release the backlog.";
}

StatusOr<lsn_t> TransactionManager::CompensateUpdateLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        slot_t slot,
                                                        std::string_view redo,
                                                        lsn_t undo_next_lsn) {
  const LogRecord lr = LogRecord::CompensateUpdatingLogRecord(
      undo_next_lsn, txn_id, pid, slot, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::AddLog(const LogRecord& lr) {
  if (fail_stop_.load(std::memory_order_acquire)) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  return logger_->AddLog(lr.Serialize());
}

uint64_t TransactionManager::CommittedLSN() const {
  return logger_->CommittedLSN();
}

}  // namespace tinylamb
