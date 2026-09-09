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
// Backstop poll for write-intent waits.  The cross-shard wound broadcasts
// (Wound-Wait and the deadlock detector) notify without the target shard's
// mutex -- acquiring every shard mutex there would need a global shard-lock
// order the code does not have -- so a waiter between its predicate check
// and its block can miss one notification.  Bounding the wait turns that
// from a permanent stall into a one-interval delay; a delivered notify
// still wakes the waiter immediately.
constexpr auto kWriteIntentPollInterval = std::chrono::milliseconds(10);
}  // namespace

TransactionManager::~TransactionManager() {
  gc_stop_.store(true, std::memory_order_release);
  {
    std::scoped_lock wake(gc_mutex_);
  }
  gc_cv_.notify_all();
  if (gc_worker_.joinable()) {
    gc_worker_.join();
  }
  // Stop the deadlock detector if it was ever started.
  deadlock_detector_stop_.store(true, std::memory_order_release);
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
  gc_worker_ = std::thread([this] { GcWorkerLoop(); });
}

void TransactionManager::AddWaitForEdge(txn_id_t waiter, txn_id_t holder,
                                        RowPosition row) {
  std::scoped_lock lk(wait_for_mu_);
  wait_for_edges_[waiter] = WaitForEdge{.row = row, .holder = holder};
}

void TransactionManager::RemoveWaitForEdge(txn_id_t waiter) {
  std::scoped_lock lk(wait_for_mu_);
  wait_for_edges_.erase(waiter);
}

void TransactionManager::RemoveWaitForEdgesOf(txn_id_t holder) {
  // Two ways a holder can disappear from the wait-for graph: it was the
  // waiter of someone else (clean up on grant), or it was the holder of
  // someone else's edge (clean up here when the holder commits/aborts).
  std::scoped_lock lk(wait_for_mu_);
  for (auto it = wait_for_edges_.begin(); it != wait_for_edges_.end();) {
    if (it->second.holder == holder || it->first == holder) {
      it = wait_for_edges_.erase(it);
    } else {
      ++it;
    }
  }
}

void TransactionManager::DeadlockDetectorLoop() {
  // Polling interval balances detection latency against wasted work when
  // the graph is empty. The cv-based wake path notifies us on every new
  // edge, so the worst-case delay is one interval.
  constexpr auto kPollInterval = std::chrono::milliseconds(2);
  while (!deadlock_detector_stop_.load(std::memory_order_acquire)) {
    std::unique_lock<std::mutex> lk(deadlock_detector_mu_);
    deadlock_detector_cv_.wait_for(lk, kPollInterval, [this] {
      if (deadlock_detector_stop_.load(std::memory_order_acquire)) {
        return true;
      }
      // The edge map is guarded by wait_for_mu_, not by the cv mutex; take
      // it here so the poll never races AddWaitForEdge/RemoveWaitForEdge.
      std::scoped_lock g(wait_for_mu_);
      return !wait_for_edges_.empty();
    });
    lk.unlock();
    if (deadlock_detector_stop_.load(std::memory_order_acquire)) {
      break;
    }
    // Snapshot the current edges under the mutex, then analyze without it.
    std::vector<std::pair<txn_id_t, txn_id_t>> edges;
    {
      std::scoped_lock g(wait_for_mu_);
      edges.reserve(wait_for_edges_.size());
      for (const auto& [w, e] : wait_for_edges_) {
        edges.emplace_back(w, e.holder);
      }
    }
    if (edges.empty()) {
      continue;
    }
    // For every waiter, walk the chain forward (waiter -> its holder -> that
    // holder's waiters' holders -> ...). When a holder equals the original
    // waiter, we have a cycle. Wound the maximum-id (youngest) participant.
    std::unordered_set<txn_id_t> already_wounded;
    for (const auto& [start, _] : edges) {
      std::unordered_set<txn_id_t> visited;
      txn_id_t cur = start;
      bool found_cycle = false;
      std::vector<txn_id_t> path;
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
        auto it = std::find_if(edges.begin(), edges.end(),
                               [cur](const std::pair<txn_id_t, txn_id_t>& e) {
                                 return e.first == cur;
                               });
        if (it == edges.end()) {
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
  constexpr auto kGcPollInterval = std::chrono::milliseconds(10);
  while (!gc_stop_.load(std::memory_order_acquire)) {
    std::unique_lock<std::mutex> lk(gc_mutex_);
    gc_cv_.wait_for(lk, kGcPollInterval, [&] {
      return gc_stop_.load(std::memory_order_acquire) ||
             commits_since_gc_.load(std::memory_order_relaxed) >=
                 kGcCommitThreshold;
    });
    if (gc_stop_.load(std::memory_order_acquire)) {
      break;
    }
    lk.unlock();
    // Nothing to do when no commit accumulated since the last pass.
    if (commits_since_gc_.exchange(0, std::memory_order_relaxed) == 0) {
      continue;
    }
    GarbageCollectVersions();
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
    active_transactions_.emplace(new_txn_id, &new_txn);
    active_snapshots_.emplace(new_txn_id, new_txn.snapshot_ts_);
  }
  assert(!new_txn.IsFinished());
  // If the return move-constructs instead of eliding into the caller,
  // Transaction's move operations repoint active_transactions_ at the new
  // address; the registration never outlives this local.
  return new_txn;
}

Status TransactionManager::PreCommit(Transaction& txn) {
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
  if (!txn.IsReadOnly()) {
    LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
    lsn_t commit_end = 0;
    {
      StatusOr<lsn_t> appended = logger_->AddLog(commit_log.Serialize());
      if (appended.HasValue()) {
        txn.prev_lsn_ = appended.MoveValue();
        // AddLog returns the LSN *before* the payload; durable point is end
        // of the buffered commit record.
        commit_end = logger_->BufferedLSN();
      } else {
        // A dead logger cannot take compensation logs, so full rollback is
        // impossible.  Still leave no half-finished state behind: release the
        // write intents (AbortVersions only resets pending owners and wakes
        // waiters -- it appends nothing), drop the registry slot so the
        // transaction cannot remain active forever, and report it as aborted,
        // never committed.  No version was published.  Skipping AbortVersions
        // here left the intents installed with a dead owner: waiters policy-
        // allowed to wait on an older holder block forever, and with the
        // wait-for edges dropped above the detector can never wound anyone.
        RemoveWaitForEdgesOf(txn.ID());
        AbortVersions(txn);
        txn.SetStatus(TransactionStatus::kAborted);
        ForgetTransaction(txn);
        return appended.GetStatus();
      }
    }
    CommitVersions(txn, commit_end);
    txn.SetStatus(TransactionStatus::kCommitted);
    Status durable = Status::kSuccess;
    if (synchronous_commit_) {
      const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
      const auto wait_start = measure ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
      durable = logger_->WaitForDurable(commit_end);
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
      // D4 barrier: the result this transaction produced depends on every
      // commit it observed, not only its own.  synchronous_commit may skip
      // the own-commit wait, but it never disables this dependency wait, and
      // a read-only transaction is subject to the same barrier.
      const lsn_t dependence = txn.DurabilityDependence();
      if (dependence != 0) {
        durable = logger_->WaitForDurable(dependence);
      }
    }
    if (durable != Status::kSuccess) {
      RemoveWaitForEdgesOf(txn.ID());
      ForgetTransaction(txn);
      txn.SetStatus(TransactionStatus::kAborted);
      return durable;
    }
  } else {
    txn.SetStatus(TransactionStatus::kCommitted);
    // D4 (docs/design.md) acceptance 3: a read-only transaction that read a
    // version published from a not-yet-durable commit must wait for that
    // commit LSN before its result is returned -- never skipped just because
    // there is no own commit record to flush.
    const lsn_t dependence = txn.DurabilityDependence();
    if (dependence != 0) {
      const Status durable = logger_->WaitForDurable(dependence);
      if (durable != Status::kSuccess) {
        // Nothing was published, but the registry slot and snapshot entry
        // must not outlive this stack-allocated transaction: a later
        // checkpoint, wounding, or deadlock scan would dereference freed
        // memory, and the orphaned snapshot would pin MVCC GC forever.
        RemoveWaitForEdgesOf(txn.ID());
        ForgetTransaction(txn);
        txn.SetStatus(TransactionStatus::kAborted);
        return durable;
      }
    }
  }
  ForgetTransaction(txn);
  return Status::kSuccess;
}

Status TransactionManager::Abort(Transaction& txn) {
  // Guard double-finish: calling Abort() after a successful PreCommit must
  // not roll back already-published committed writes (the undo walk would
  // silently revert data other transactions may already have read).
  if (txn.IsFinished()) {
    return Status::kSuccess;
  }
  if (txn.IsReadOnly()) {
    txn.SetStatus(TransactionStatus::kAborted);
    RemoveWaitForEdgesOf(txn.ID());
    ForgetTransaction(txn);
    return Status::kSuccess;
  }
  // Drop our hold on every wait-for edge before undoing: any later wait
  // waking up to find no holder should not see this transaction as a
  // possible victim candidate.
  RemoveWaitForEdgesOf(txn.ID());
  // Wait on the logger's durability condition variable instead of polling:
  // the worker wakes every waiter once records are fsynced, and a dead
  // logger (Failed()) also releases the wait.  A failure falls through to
  // the abort-log write below, which surfaces it.
  {
    // Wait for the *end* of this transaction's last record, not the record
    // start: AddLog returns the LSN before the payload, so waiting on
    // prev_lsn_ alone could let the undo walk below read a record whose tail
    // is still in the logger buffer (strace-confirmed source of the -j32
    // "Invalid format log" flakiness).
    const uint64_t latest_log_end = logger_->BufferedLSN();
    const Status durable = logger_->WaitForDurable(latest_log_end);
    if (durable != Status::kSuccess) {
      // Fall through; the undo replay reads what was flushed and the abort
      // log write reports the broken WAL.
      LOG(WARN) << "WAL durability wait failed during abort: " << durable;
    }
  }
  lsn_t prev = txn.prev_lsn_;
  // The undo walk can fail (corrupt page hand-back, dead logger).  A failure
  // must not skip AbortVersions/SetStatus: intents left installed plus a
  // transaction that stays active would block its rows for the process
  // lifetime.  Clean up, then report the failure.
  Status undo_status = Status::kSuccess;
  while (prev != 0) {
    LogRecord lr;
    if (!recovery_->ReadLog(prev, &lr)) {
      // The record never became durable (logger failure path above) or the
      // tail was truncated: there is nothing on this chain left to undo.
      LOG(WARN) << "Abort undo: unreadable log at " << prev << ", stopping";
      break;
    }
    undo_status =
        recovery_->LogUndoWithPage(prev, lr, txn.transaction_manager_);
    if (undo_status != Status::kSuccess) {
      break;
    }
    prev = lr.prev_lsn;
  }
  AbortVersions(txn);
  txn.SetStatus(TransactionStatus::kAborted);
  LogRecord abort_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
  StatusOr<lsn_t> appended = logger_->AddLog(abort_log.Serialize());
  // Same contract as PreCommit: release locks and leave an aborted state
  // rather than a half-finished transaction blocking everyone.
  if (appended.HasValue()) {
    txn.prev_lsn_ = appended.MoveValue();
  } else if (undo_status == Status::kSuccess) {
    undo_status = appended.GetStatus();
  }
  ForgetTransaction(txn);
  return undo_status;
}

bool TransactionManager::AcquireWriteIntent(
    Transaction& txn, const RowPosition& rp, bool wait,
    std::optional<std::string_view> before) {
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
  // Try-lock path: caller (TryAddWriteSet) does not want to wait, regardless
  // of the policy. The current pending owner is enough information to make
  // the decision.
  if (!wait) {
    if (available_for_self()) {
      VersionChain& chain = shard.versions[rp];
      if (chain.pending) {
        return chain.pending->owner == txn.ID();
      }
      chain.pending = PendingVersion{
          .owner = txn.ID(), .value = std::nullopt, .staged = false};
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
        return false;
      }
      if (txn.IsWounded()) {
        return false;
      }
      break;
    }
    case DeadlockPolicy::kWaitDie: {
      // If the holder is younger than us, the policy says we must die.  We
      // never wound a younger holder; we just refuse to wait and let the
      // caller abort us. This bounds wait-queue length to a single older
      // waiter per row and is deadlock-free by construction.
      const txn_id_t current_holder = holder_id();
      if (current_holder != 0 && current_holder != txn.ID() &&
          txn.ID() > current_holder) {
        if (measure) {
          write_intent_conflicts_.fetch_add(1, std::memory_order_relaxed);
        }
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
      while (!available()) {
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      if (txn.IsWounded()) {
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
      while (!available()) {
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      if (txn.IsWounded()) {
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
        AddWaitForEdge(txn.ID(), current_holder, rp);
        // Wake the detector: a new edge may have created a cycle.
        deadlock_detector_cv_.notify_all();
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
      while (!available()) {
        shard.write_intent_released.wait_for(lock, kWriteIntentPollInterval);
      }
      // Whether we got the lock or were wounded, our wait-for edge is moot.
      RemoveWaitForEdge(txn.ID());
      if (txn.IsWounded()) {
        return false;
      }
      break;
    }
  }
  VersionChain& chain = shard.versions[rp];
  if (chain.pending) {
    return chain.pending->owner == txn.ID();
  }
  // Strict write locking: a conflicting writer waits, then evaluates against
  // the latest physical row after reserving its own unstaged intent. This is
  // the TPC-C isolation behavior (T2 waits and completes), not SI's
  // first-updater-wins abort.
  chain.pending =
      PendingVersion{.owner = txn.ID(), .value = std::nullopt, .staged = false};
  if (before && chain.committed.empty()) {
    chain.committed.push_back(
        {0, std::numeric_limits<uint64_t>::max(),
         std::optional<std::string>(std::string(*before))});
  }
  return true;
}

TransactionRuntimeStats TransactionManager::RuntimeStats() const {
  return {
      .wal_wait_count = wal_wait_count_.load(std::memory_order_relaxed),
      .wal_wait_ns = wal_wait_ns_.load(std::memory_order_relaxed),
      .write_intent_attempts =
          write_intent_attempts_.load(std::memory_order_relaxed),
      .write_intent_conflicts =
          write_intent_conflicts_.load(std::memory_order_relaxed),
      .write_intent_mutex_wait_ns =
          write_intent_mutex_wait_ns_.load(std::memory_order_relaxed),
      .commit_shard_mutex_wait_ns =
          commit_shard_mutex_wait_ns_.load(std::memory_order_relaxed),
  };
}

StatusOr<std::string> TransactionManager::ReadVersion(
    const Transaction& txn, const RowPosition& rp,
    std::optional<std::string_view> physical) const {
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    if (!physical) {
      return Status::kNotExists;
    }
    return std::string(*physical);
  }
  const VersionChain& chain = found->second;
  if (chain.pending && chain.pending->owner == txn.ID() &&
      chain.pending->staged) {
    if (!chain.pending->value) {
      return Status::kNotExists;
    }
    return *chain.pending->value;
  }
  // A writer that waited for a predecessor implements strict write locking,
  // not first-updater-wins snapshot isolation: its SET expression must see
  // the predecessor's committed result.  The newest committed entry is kept
  // while this unstaged intent exists.  When GC removed a redundant chain
  // before the intent was acquired, the heap is the authoritative latest
  // image and the physical fallback below is equivalent.
  if (chain.pending && chain.pending->owner == txn.ID()) {
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
      return std::string(*physical);
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

bool TransactionManager::HasVersionChain(const RowPosition& rp) const {
  const VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  return shard.versions.contains(rp);
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
  VersionChain& chain = shard.versions[rp];
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

bool TransactionManager::IndexKeysMayBeStale(const Transaction& txn) const {
  // O(1) IndexScan plan gate: only committed index mutations can hide keys.
  // Concurrent pending writers are resolved per row via ReadVersion.
  return txn.SnapshotTimestamp() <
         max_committed_begin_ts_.load(std::memory_order_acquire);
}

bool TransactionManager::IndexKeysMayBeStale(const Transaction& txn,
                                             page_id_t index_root) const {
  return txn.SnapshotTimestamp() <
         max_index_mutation_ts_[IndexMutationShardIndex(index_root)].load(
             std::memory_order_acquire);
}

bool TransactionManager::RequiresHistoricalRead(const Transaction& txn) const {
  return IndexKeysMayBeStale(txn);
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
  const uint64_t current = stable_timestamp_.load(std::memory_order_relaxed);
  if (current < next_stable) {
    stable_timestamp_.store(next_stable, std::memory_order_release);
  }
}

void TransactionManager::CommitVersions(Transaction& txn, lsn_t commit_lsn) {
  // Publish row versions under shard locks alone; the timestamp itself is
  // allocated and registered unpublished under pending_commits_mutex_.  A
  // concurrent Begin() takes its snapshot from stable_timestamp_, which only
  // advances to fully published timestamps, so no global lock serializes
  // commits anymore.
  std::array<bool, kVersionShardCount> needed{};
  for (const RowPosition& rp : txn.write_set_) {
    needed[VersionShardIndex(rp)] = true;
  }
  const uint64_t commit_ts = AllocatePendingCommit();
  std::vector<std::unique_lock<std::mutex>> shard_locks;
  shard_locks.reserve(kVersionShardCount);
  for (size_t i = 0; i < kVersionShardCount; ++i) {
    if (needed[i]) {
      const bool measure = metrics_enabled_.load(std::memory_order_relaxed);
      const auto wait_start = measure ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
      shard_locks.emplace_back(version_shards_[i].mutex);
      if (measure) {
        commit_shard_mutex_wait_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - wait_start)
                    .count()),
            std::memory_order_relaxed);
      }
    }
  }
  for (const RowPosition& rp : txn.write_set_) {
    VersionShard& shard = version_shards_[VersionShardIndex(rp)];
    const auto found = shard.versions.find(rp);
    if (found == shard.versions.end()) {
      continue;
    }
    VersionChain& chain = found->second;
    if (!chain.pending || chain.pending->owner != txn.ID()) {
      continue;
    }
    if (!chain.pending->staged) {
      chain.pending.reset();
      shard.write_intent_released.notify_all();
      continue;
    }
    if (!chain.committed.empty()) {
      chain.committed.back().end_ts = commit_ts;
    }
    chain.committed.push_back({commit_ts, std::numeric_limits<uint64_t>::max(),
                               std::move(chain.pending->value), commit_lsn});
    chain.pending.reset();
    shard.write_intent_released.notify_all();
  }
  shard_locks.clear();
  max_committed_begin_ts_.store(
      std::max(max_committed_begin_ts_.load(std::memory_order_relaxed),
               commit_ts),
      std::memory_order_release);
  for (page_id_t root : txn.mutated_index_roots_) {
    std::atomic<uint64_t>& latest =
        max_index_mutation_ts_[IndexMutationShardIndex(root)];
    uint64_t observed = latest.load(std::memory_order_relaxed);
    while (observed < commit_ts &&
           !latest.compare_exchange_weak(observed, commit_ts,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
    }
  }
  // From here on every snapshot >= commit_ts observes this publication.  If
  // the (allocation-free in practice) publication above ever throws,
  // commit_ts stays registered as unpublished: correct (snapshots stall at
  // the previous stable point), just conservatively slow.
  PublishCommit(commit_ts);
}

void TransactionManager::AbortVersions(Transaction& txn) {
  std::array<bool, kVersionShardCount> needed{};
  for (const RowPosition& rp : txn.write_set_) {
    needed[VersionShardIndex(rp)] = true;
  }
  std::vector<std::unique_lock<std::mutex>> shard_locks;
  shard_locks.reserve(kVersionShardCount);
  for (size_t i = 0; i < kVersionShardCount; ++i) {
    if (needed[i]) {
      shard_locks.emplace_back(version_shards_[i].mutex);
    }
  }
  for (const RowPosition& rp : txn.write_set_) {
    VersionShard& shard = version_shards_[VersionShardIndex(rp)];
    const auto found = shard.versions.find(rp);
    if (found != shard.versions.end() && found->second.pending &&
        found->second.pending->owner == txn.ID()) {
      found->second.pending.reset();
      shard.write_intent_released.notify_all();
    }
  }
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
      commits_since_gc_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void TransactionManager::ForgetTransaction(Transaction& txn) {
  {
    std::scoped_lock lk(transaction_table_lock);
    if (!active_transactions_.empty()) {
      active_transactions_.erase(txn.txn_id_);
    }
    active_snapshots_.erase(txn.txn_id_);
  }
  // GC no longer runs on the commit critical path: the background worker
  // picks the work up (threshold-driven) within a few milliseconds.
  if (!txn.IsReadOnly()) {
    commits_since_gc_.fetch_add(1, std::memory_order_relaxed);
  }
}

void TransactionManager::GarbageCollectVersions() {
  std::optional<uint64_t> oldest_snapshot;
  {
    std::scoped_lock lock(transaction_table_lock);
    for (const auto& [id, snapshot] : active_snapshots_) {
      if (!oldest_snapshot || snapshot < *oldest_snapshot) {
        oldest_snapshot = snapshot;
      }
    }
  }
  for (VersionShard& shard : version_shards_) {
    std::scoped_lock lock(shard.mutex);
    for (auto chain_iter = shard.versions.begin();
         chain_iter != shard.versions.end();) {
      VersionChain& chain = chain_iter->second;
      // The heap page is the authoritative latest image. Once no active
      // snapshot can need an older image, retaining the latest committed
      // value here is both redundant and disastrous for OLTP: GC would scan
      // every row ever loaded on every pass. A deleted row needs no tombstone
      // for future snapshots either -- its physical slot/index entry is gone.
      if (!chain.pending &&
          (!oldest_snapshot || chain.committed.empty() ||
           chain.committed.back().begin_ts <= *oldest_snapshot)) {
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
        chain_iter = shard.versions.erase(chain_iter);
        continue;
      }
      ++chain_iter;
    }
  }
}

StatusOr<lsn_t> TransactionManager::CompensateInsertLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        slot_t slot) {
  const LogRecord lr =
      LogRecord::CompensatingInsertLogRecord(txn_id, pid, slot);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}
StatusOr<lsn_t> TransactionManager::CompensateInsertLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        std::string_view key) {
  const LogRecord lr = LogRecord::CompensatingInsertLogRecord(txn_id, pid, key);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}
StatusOr<lsn_t> TransactionManager::CompensateInsertBranchLog(
    txn_id_t txn_id, page_id_t pid, std::string_view key) {
  const LogRecord lr =
      LogRecord::CompensatingInsertBranchLogRecord(txn_id, pid, key);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateUpdateLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        slot_t slot,
                                                        std::string_view redo) {
  const LogRecord lr =
      LogRecord::CompensatingUpdateLogRecord(txn_id, pid, slot, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}
StatusOr<lsn_t> TransactionManager::CompensateUpdateLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        std::string_view key,
                                                        std::string_view redo) {
  const LogRecord lr =
      LogRecord::CompensatingUpdateLeafLogRecord(txn_id, pid, key, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}
StatusOr<lsn_t> TransactionManager::CompensateUpdateBranchLog(
    txn_id_t txn_id, page_id_t pid, std::string_view key, page_id_t redo) {
  const LogRecord lr =
      LogRecord::CompensatingUpdateBranchLogRecord(txn_id, pid, key, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateDeleteLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        slot_t slot,
                                                        std::string_view redo) {
  const LogRecord lr =
      LogRecord::CompensatingDeleteLogRecord(txn_id, pid, slot, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateDeleteLog(txn_id_t txn_id,
                                                        page_id_t pid,
                                                        std::string_view key,
                                                        std::string_view redo) {
  const LogRecord lr =
      LogRecord::CompensatingDeleteLeafLogRecord(txn_id, pid, key, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateDeleteBranchLog(
    txn_id_t txn_id, page_id_t pid, std::string_view key, page_id_t redo) {
  const LogRecord lr =
      LogRecord::CompensatingDeleteBranchLogRecord(txn_id, pid, key, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateSetLowestValueLog(
    txn_id_t txn_id, page_id_t pid, page_id_t redo) {
  const LogRecord lr =
      LogRecord::CompensateSetLowestValueLogRecord(txn_id, pid, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateSetLowFenceLog(
    txn_id_t txn_id, page_id_t pid, const IndexKey& redo) {
  const LogRecord lr =
      LogRecord::CompensateSetLowFenceLogRecord(0, txn_id, pid, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateSetHighFenceLog(
    txn_id_t txn_id, page_id_t pid, const IndexKey& redo) {
  const LogRecord lr =
      LogRecord::CompensateSetHighFenceLogRecord(0, txn_id, pid, redo);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::CompensateSetFosterLog(
    txn_id_t txn_id, page_id_t pid, const FosterPair& foster) {
  const LogRecord lr =
      LogRecord::CompensateSetFosterLogRecord(0, txn_id, pid, foster);
  ASSIGN_OR_RETURN(lsn_t, start, AddLog(lr));
  return start + lr.Size();
}

StatusOr<lsn_t> TransactionManager::AddLog(const LogRecord& lr) {
  return logger_->AddLog(lr.Serialize());
}

uint64_t TransactionManager::CommittedLSN() const {
  return logger_->CommittedLSN();
}

}  // namespace tinylamb
