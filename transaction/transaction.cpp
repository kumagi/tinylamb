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

#include "transaction/transaction.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "page/page_manager.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t) {
  switch (t) {
    case TransactionStatus::kUnknown:
      o << "Unknown";
      break;
    case TransactionStatus::kRunning:
      o << "Running";
      break;
    case TransactionStatus::kCommitted:
      o << "Committed";
      break;
    case TransactionStatus::kAborted:
      o << "Aborted";
      break;
  }
  return o;
}

Transaction::~Transaction() noexcept { FinishRunningNoexcept(); }

void Transaction::FinishRunningNoexcept() noexcept {
  if (!IsFinished()) {
    TransactionManager* const manager = transaction_manager_;
    if (manager != nullptr) {
      if (manager->recovery_ != nullptr) {
        try {
          (void)manager->Abort(*this);
        } catch (...) {
          failed_.store(true, std::memory_order_release);
        }
      }
      if (!IsFinished()) {
        if (manager->recovery_ == nullptr && HasLogRecord() &&
            manager->page_manager_ != nullptr) {
          manager->fail_stop_.store(true, std::memory_order_release);
        }
        if (manager->page_manager_ != nullptr && !staged_records_.empty()) {
          try {
            manager->page_manager_->ApplyTransactionAbort(txn_id_,
                                                          staged_records_);
            staged_records_.clear();
          } catch (...) {
            failed_.store(true, std::memory_order_release);
          }
        }
        try {
          manager->AbortVersions(*this);
        } catch (...) {
          failed_.store(true, std::memory_order_release);
        }
        status_.store(TransactionStatus::kAborted, std::memory_order_release);
        try {
          manager->ForgetTransaction(*this);
        } catch (...) {
          failed_.store(true, std::memory_order_release);
        }
      }
    } else {
      status_.store(TransactionStatus::kAborted, std::memory_order_release);
    }
  }
  FinalizeFinishHooks();
}

void Transaction::FinalizeFinishHooks() noexcept {
  if (finish_hooks_finalized_ || !IsFinished()) {
    return;
  }
  finish_hooks_finalized_ = true;
  std::vector<std::shared_ptr<TransactionFinishHook>> hooks;
  hooks.swap(finish_hooks_);
  const TransactionStatus status = status_.load(std::memory_order_acquire);
  for (const auto& hook : hooks) {
    if (hook == nullptr) {
      continue;
    }
    try {
      hook->OnTransactionFinished(status);
    } catch (...) {  // NOLINT(bugprone-empty-catch)
      // Hooks are observer callbacks: a throwing hook must not fail (or
      // abort) the transaction finish path, so swallow and move on.
      continue;
    }
  }
}

Transaction::Transaction(txn_id_t txn_id, TransactionManager* tm,
                         bool read_only)
    : txn_id_(txn_id),
      // Begin() immediately overwrites this with StableTimestamp() under the
      // registry lock; the value here is a placeholder only (the stable
      // timestamp cannot be read safely before registration).
      snapshot_ts_(0),
      shard_epoch_(NextShardEpoch()),
      status_(TransactionStatus::kRunning),
      read_only_(read_only),
      transaction_manager_(tm) {}

// Begin() registers the address of the Transaction it is about to return.
// NRVO usually makes that address the final one; when the return instead
// move-constructs (e.g. Database::BeginContext moves the result into
// TransactionContext::txn_), follow the registration so the manager's
// active_transactions_ map never points at the moved-from object.
Transaction::Transaction(Transaction&& o) noexcept
    : txn_id_(o.txn_id_),
      snapshot_ts_(o.snapshot_ts_),
      read_set_(std::move(o.read_set_)),
      write_set_(std::move(o.write_set_)),
      recycled_page_allocations_(std::move(o.recycled_page_allocations_)),
      mutated_index_roots_(std::move(o.mutated_index_roots_)),
      index_undo_reservations_(std::move(o.index_undo_reservations_)),
      staged_records_(std::move(o.staged_records_)),
      shard_epoch_(o.shard_epoch_),
      write_epoch_(o.write_epoch_.load(std::memory_order_acquire)),
      version_read_caches_(std::move(o.version_read_caches_)),
      read_state_mutex_(std::move(o.read_state_mutex_)),
      serialization_scratch_(std::move(o.serialization_scratch_)),
      // chain_mu_ is not in the init list on purpose: it stays per-object.
      prev_lsn_(o.prev_lsn_.load(std::memory_order_relaxed)),
      prev_end_lsn_(o.prev_end_lsn_.load(std::memory_order_relaxed)),
      first_lsn_(o.first_lsn_.load(std::memory_order_relaxed)),
      has_log_(o.has_log_.load(std::memory_order_relaxed)),
      has_table_drop_(o.has_table_drop_.load(std::memory_order_acquire)),
      finish_hooks_(std::move(o.finish_hooks_)),
      finish_hooks_finalized_(o.finish_hooks_finalized_),
      durability_dependence_(
          o.durability_dependence_.load(std::memory_order_acquire)),
      status_(o.status_.load(std::memory_order_relaxed)),
      failed_(o.failed_.load(std::memory_order_acquire)),
      read_only_(o.read_only_),
      transaction_manager_(o.transaction_manager_),
      wounded_(o.wounded_.load(std::memory_order_acquire)) {
  debug_phase_.store(o.debug_phase_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  debug_tid_.store(o.debug_tid_.load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
  if (o.transaction_manager_ != nullptr) {
    o.transaction_manager_->MoveActiveTransaction(&o, this);
  }
  // Neutralize the moved-from object: it no longer owns a registry slot or
  // manager services (mirrors PageRef's move convention).
  o.transaction_manager_ = nullptr;
  o.finish_hooks_.clear();
  o.finish_hooks_finalized_ = true;
}

Status Transaction::PreCommit() { return PreCommitImpl(nullptr); }

StatusOr<lsn_t> Transaction::PrepareCommitForDurableReply() {
  lsn_t target = 0;
  RETURN_IF_FAIL(PreCommitImpl(&target));
  return target;
}

Status Transaction::PreCommitImpl(lsn_t* external_durability_target) {
  if (IsFinished()) {
    return Status::kRuntimeError;
  }
  if (IsFailed()) {
    try {
      (void)Abort();
    } catch (...) {
      failed_.store(true, std::memory_order_release);
    }
    FinishRunningNoexcept();
    return Status::kRuntimeError;
  }
  if (transaction_manager_ == nullptr) {
    return Status::kRuntimeError;
  }
  Status result =
      transaction_manager_->PreCommit(*this, external_durability_target);
  // Adopt the manager-assigned status: the pre-publish AddLog-failure path
  // reports kAborted, while the post-publish durability-failure path keeps
  // kCommitted (published versions cannot be retracted).  Overwriting a
  // failed-but-committed outcome with kAborted here would let a later Abort()
  // roll back data other transactions may already have read.
  if (IsFinished()) {
    return result;
  }
  if (result == Status::kSuccess) {
    status_ = TransactionStatus::kCommitted;
  } else {
    // A failed PreCommit must not be reported as committed.
    FinishRunningNoexcept();
  }
  return result;
}

Status Transaction::Abort() {
  if (IsFinished()) {
    return Status::kSuccess;
  }
  if (transaction_manager_ == nullptr) {
    status_.store(TransactionStatus::kAborted, std::memory_order_release);
    FinalizeFinishHooks();
    return Status::kRuntimeError;
  }
  if (transaction_manager_->recovery_ == nullptr) {
    if (HasLogRecord() && transaction_manager_->page_manager_ != nullptr) {
      transaction_manager_->fail_stop_.store(true, std::memory_order_release);
    }
    if (transaction_manager_->page_manager_ != nullptr &&
        !staged_records_.empty()) {
      transaction_manager_->page_manager_->ApplyTransactionAbort(
          txn_id_, staged_records_);
      staged_records_.clear();
    }
    transaction_manager_->AbortVersions(*this);
    status_.store(TransactionStatus::kAborted, std::memory_order_release);
    transaction_manager_->ForgetTransaction(*this);
    FinalizeFinishHooks();
    return HasLogRecord() ? Status::kRuntimeError : Status::kSuccess;
  }
  return transaction_manager_->Abort(*this);
}

bool Transaction::AddReadSet(const RowPosition& rp) {
  assert(!IsFinished());
  // MV2PL readers use snapshot-visible row versions and therefore never take
  // a lock that conflicts with a writer's exclusive lock.
  // Hash joins may consume both children concurrently.  Index scans on those
  // children register their reads against the same transaction, so protect
  // the diagnostic set just like the version-cache state.
  std::scoped_lock state_guard(*read_state_mutex_);
  read_set_.insert(rp);
  return true;
}

bool Transaction::AddWriteSet(const RowPosition& rp) {
  assert(!IsFinished());
  // IsFinished() must be enforced, not just asserted: a stale caller that
  // keeps writing through a committed transaction would install a permanent
  // write intent that no PreCommit/Abort can ever release (both short-circuit
  // on a finished status), blocking that row for the life of the database.
  if (IsFailed() || IsFinished() || read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    return true;
  }
  write_set_.insert(rp);
  try {
    if (transaction_manager_->AcquireWriteIntent(*this, rp, true)) {
      return true;
    }
  } catch (...) {
    transaction_manager_->ReleaseWriteIntent(*this, rp);
    write_set_.erase(rp);
    throw;
  }
  write_set_.erase(rp);
  return false;
}

bool Transaction::AddWriteSet(const RowPosition& rp, std::string_view before) {
  assert(!IsFinished());
  if (IsFailed() || IsFinished() || read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    // The write set may have pre-reserved this row without a before-image
    // (TryAddWriteSet); re-acquire so the image is recorded and concurrent
    // snapshots keep seeing the pre-update value.
    return transaction_manager_->AcquireWriteIntent(*this, rp, true, before);
  }
  write_set_.insert(rp);
  try {
    if (transaction_manager_->AcquireWriteIntent(*this, rp, true, before)) {
      return true;
    }
  } catch (...) {
    transaction_manager_->ReleaseWriteIntent(*this, rp);
    write_set_.erase(rp);
    throw;
  }
  write_set_.erase(rp);
  return false;
}

bool Transaction::TryAddWriteSet(const RowPosition& rp) {
  assert(!IsFinished());
  if (IsFailed() || IsFinished() || read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    return true;
  }
  write_set_.insert(rp);
  try {
    if (transaction_manager_->AcquireWriteIntent(*this, rp, false)) {
      return true;
    }
  } catch (...) {
    transaction_manager_->ReleaseWriteIntent(*this, rp);
    write_set_.erase(rp);
    throw;
  }
  write_set_.erase(rp);
  return false;
}

bool Transaction::SnapshotSeesRow(const RowPosition& rp) const {
  return transaction_manager_->SnapshotSeesRow(*this, rp);
}

void Transaction::ReleaseWriteIntent(const RowPosition& rp) {
  if (!transaction_manager_->ReleaseWriteIntent(*this, rp)) {
    return;
  }
  // The reservation is gone from the chain; keeping rp in the write set
  // would let a later write on the same slot skip AcquireWriteIntent and
  // reach RegisterVersionWrite with no pending intent.
  write_set_.erase(rp);
}

Transaction::VersionCacheShard& Transaction::ThreadShard() {
  // Thread-local shortcut so the shard-map lock is taken once per thread
  // instead of once per row.  The process-wide epoch guards against stale
  // entries when a destroyed transaction's address or id is reused.
  thread_local uint64_t owner_epoch = 0;
  thread_local VersionCacheShard* shard = nullptr;
  if (owner_epoch == shard_epoch_ && shard != nullptr) {
    return *shard;
  }
  std::scoped_lock state_guard(*read_state_mutex_);
  std::unique_ptr<VersionCacheShard>& entry =
      version_read_caches_[std::this_thread::get_id()];
  if (!entry) {
    entry = std::make_unique<VersionCacheShard>();
  }
  owner_epoch = shard_epoch_;
  shard = entry.get();
  return *shard;
}

uint64_t Transaction::NextShardEpoch() {
  static std::atomic<uint64_t> counter{1};
  return counter.fetch_add(1, std::memory_order_relaxed);
}

StatusOr<std::string_view> Transaction::ReadVersion(
    const RowPosition& rp, std::optional<std::string_view> physical,
    bool resolve_head) {
  assert(!IsFinished());
  if (transaction_manager_ != nullptr && transaction_manager_->FailStopped()) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  // Fast path: a row whose version shard carries no chain has exactly one
  // version -- the physical image inside the caller's pinned page -- which
  // every snapshot sees as-is.  Return it directly: no copy, no cache entry,
  // and no locks.  One acquire load instead of a shard-mutex probe; rows on
  // chain-bearing pages take the fused path below (TransactionManager::
  // ReadVersion resolves them under a single shard lock).
  if (transaction_manager_ != nullptr && physical &&
      transaction_manager_->VersionShardIdle(rp)) {
    return *physical;
  }
  // Note: read_set_ is diagnostic bookkeeping only (MV2PL readers never
  // conflict with writers), so this path does not populate it; per-row
  // insertion under a shared lock would serialize concurrent scan workers.
  if (transaction_manager_ == nullptr) {
    if (!physical) {
      return Status::kNotExists;
    }
    return *physical;
  }
  {
    // Cache hit for this thread.  Only read-only transactions may serve a
    // cached value directly: they never write, so an epoch-current entry is
    // provably identical to what the manager would return for the fixed
    // snapshot.  Read-write transactions always consult the manager below so
    // their own in-flight writes are never masked by any shard's cache --
    // this is the cross-worker read-your-writes guarantee.
    if (read_only_) {
      VersionCacheShard& shard = ThreadShard();
      std::scoped_lock shard_lock(shard.mutex);
      const auto found = shard.entries.find(rp);
      if (found != shard.entries.end() &&
          CacheEntryCurrent(found->second.epoch)) {
        return std::string_view(found->second.value);
      }
    }
  }
  StatusOr<std::optional<std::string>> visible =
      transaction_manager_->ReadVersion(*this, rp, physical, resolve_head);
  if (!visible.HasValue()) {
    return visible.GetStatus();
  }
  if (!visible.Value().has_value()) {
    // The manager served the latched page image: the row's authoritative
    // version equals *physical byte for byte. Skip the read cache entirely
    // -- the view's lifetime is the caller's page pin, and a scan never
    // re-reads a position, so caching would only add a string copy, an
    // insert and an eviction per row.  (The manager only answers nullopt
    // when it was handed a physical image; a missing one is kNotExists.)
    if (!physical) {
      return Status::kNotExists;
    }
    return *physical;
  }
  VersionCacheShard& shard = ThreadShard();
  std::scoped_lock shard_lock(shard.mutex);
  const uint64_t epoch = write_epoch_.load(std::memory_order_acquire);
  // Retire the previously cached string for this key (if any): a caller may
  // still hold a view into it from an earlier read of the same row.
  auto existing = shard.entries.find(rp);
  if (existing != shard.entries.end()) {
    shard.retired.push_back(std::move(existing->second.value));
  }
  std::optional<std::string> served = std::move(visible).MoveValue();
  if (!served) {
    // Unreachable: the nullopt branch above already returned a view of the
    // physical image, so a disengaged optional cannot get here.
    return Status::kNotExists;
  }
  auto [iter, inserted] = shard.entries.insert_or_assign(
      rp, VersionCacheEntry{.epoch = epoch, .value = std::move(*served)});
  constexpr size_t kMaxVersionReadCache = 4096;
  constexpr size_t kMaxRetiredViews = 4096;
  if (shard.entries.size() > kMaxVersionReadCache) {
    // Evict a single entry (never the one just inserted): the retired pool
    // keeps the evicted string readable for callers that still hold a view
    // into it.
    auto victim = shard.entries.begin();
    if (victim == iter) {
      ++victim;
    }
    shard.retired.push_back(std::move(victim->second.value));
    shard.entries.erase(victim);
  }
  // The retired pool exists so recently evicted strings stay alive while a
  // consumer finishes with their views.  Once it grows past its bound the
  // oldest half is beyond any plausible in-flight view window and is freed;
  // this bounds total cache memory at roughly twice the live-entry budget.
  if (shard.retired.size() > kMaxRetiredViews) {
    shard.retired.erase(shard.retired.begin(),
                        shard.retired.begin() +
                            static_cast<std::ptrdiff_t>(
                                shard.retired.size() - (kMaxRetiredViews / 2)));
  }
  return std::string_view(iter->second.value);
}

void Transaction::RegisterVersionWrite(const RowPosition& rp,
                                       std::optional<std::string_view> before,
                                       std::optional<std::string_view> after) {
  std::scoped_lock state_guard(*read_state_mutex_);
  if (transaction_manager_ == nullptr) {
    return;
  }
  // Invalidate every shard's cached entries for this generation: any worker
  // under this transaction that reads after the new version registers must
  // not serve a pre-write value from its cache.  Entries tagged with the
  // older epoch are simply skipped; they are never freed out from under
  // their readers.
  write_epoch_.fetch_add(1, std::memory_order_release);
  transaction_manager_->RegisterVersionWrite(*this, rp, before, after);
  // Drop this thread's own copy eagerly to bound memory; other threads'
  // shards keep theirs until natural eviction, neutralized by the epoch tag.
  auto found = version_read_caches_.find(std::this_thread::get_id());
  if (found != version_read_caches_.end()) {
    std::scoped_lock shard_lock(found->second->mutex);
    found->second->entries.erase(rp);
  }
}

bool Transaction::CancelStagedVersion(const RowPosition& rp) {
  if (transaction_manager_ == nullptr) {
    write_set_.erase(rp);
    return true;
  }
  bool canceled = false;
  {
    auto& shard =
        transaction_manager_
            ->version_shards_[tinylamb::TransactionManager::VersionShardIndex(
                rp)];
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.versions.find(rp);
    if (found == shard.versions.end()) {
      canceled = true;
    } else if (found->second.pending && found->second.pending->owner == ID() &&
               found->second.pending->staged) {
      found->second.pending.reset();
      shard.write_intent_released.notify_all();
      canceled = true;
      if (found->second.committed.empty()) {
        transaction_manager_->ForgetVersionChain(rp);
        shard.versions.erase(found);
      }
    }
  }
  if (!canceled) {
    return false;
  }
  write_set_.erase(rp);
  InvalidateVersionReadCache();
  return true;
}

bool Transaction::RequiresHistoricalRead() const {
  return transaction_manager_ != nullptr &&
         transaction_manager_->RequiresHistoricalRead(*this);
}

bool Transaction::IndexKeysMayBeStale() const {
  return transaction_manager_ != nullptr &&
         transaction_manager_->IndexKeysMayBeStale(*this);
}

bool Transaction::IndexKeysMayBeStale(page_id_t index_root) const {
  return transaction_manager_ != nullptr &&
         transaction_manager_->IndexKeysMayBeStale(*this, index_root);
}

StatusOr<lsn_t> Transaction::AppendLog(const LogRecord& lr) {
  if (read_only_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "read-only transaction cannot append WAL");
  }
  if (IsFinished()) {
    return Status::kRuntimeError;
  }
  if (IsWounded()) {
    return Status::kConflicts;
  }
  if (IsFailed()) {
    return Status::kRuntimeError;
  }
  if (transaction_manager_ != nullptr && transaction_manager_->FailStopped()) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  // The per-transaction chain mutex (not a manager-wide lock): it makes
  // {record append, prev_lsn/first_lsn stores} atomic with respect to the
  // checkpoint's active-transaction snapshot while leaving unrelated
  // transactions' appends fully parallel.
  std::scoped_lock chain_guard(chain_mu_);
  if (IsFinished()) {
    return Status::kRuntimeError;
  }
  if (IsWounded()) {
    return Status::kConflicts;
  }
  DebugSetPhase("wal-append");
  LogRecord record = lr;
  record.prev_lsn = prev_lsn_.load(std::memory_order_relaxed);
  serialization_scratch_.clear();
  record.SerializeTo(serialization_scratch_);
  ASSIGN_OR_RETURN(
      lsn_t, start,
      transaction_manager_->logger_->AddLog(serialization_scratch_));
  prev_lsn_ = start;
  if (!has_log_.load(std::memory_order_relaxed)) {
    first_lsn_.store(start, std::memory_order_relaxed);
  }
  has_log_.store(true, std::memory_order_release);
  const std::string& data = serialization_scratch_;
  prev_end_lsn_ = start + static_cast<lsn_t>(data.size());
  record.record_lsn = start;
  if (record.type != LogType::kBegin && record.type != LogType::kCommit &&
      record.type != LogType::kAbort &&
      record.type != LogType::kBeginCheckpoint &&
      record.type != LogType::kEndCheckpoint) {
    staged_records_.push_back(std::move(record));
  }
  DebugSetPhase(nullptr);
  return start;
}

StatusOr<lsn_t> Transaction::InsertLog(page_id_t pid, slot_t slot,
                                       std::string_view redo) {
  return AppendLog(
      LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pid, slot, redo));
}

StatusOr<lsn_t> Transaction::UpdateLog(page_id_t pid, slot_t slot,
                                       std::string_view redo,
                                       std::string_view undo) {
  return AppendLog(
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pid, slot, redo, undo));
}

StatusOr<lsn_t> Transaction::AppendPhysicalUpdateCompensation(
    page_id_t pid, slot_t slot, std::string_view redo) {
  return AppendLog(LogRecord::CompensateUpdatingLogRecord(prev_lsn_, txn_id_,
                                                          pid, slot, redo));
}

StatusOr<lsn_t> Transaction::DeleteLog(page_id_t pid, slot_t slot,
                                       std::string_view undo) {
  return AppendLog(
      LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pid, slot, undo));
}

StatusOr<lsn_t> Transaction::SetRowPageNextLog(page_id_t pid, page_id_t redo,
                                               page_id_t undo) {
  return AppendLog(
      LogRecord::SetRowPageNextLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::SetRowPagePrevLog(page_id_t pid, page_id_t redo,
                                               page_id_t undo) {
  return AppendLog(
      LogRecord::SetRowPagePrevLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::LsmPutLog(page_id_t root, std::string_view key,
                                       std::string_view value,
                                       std::string_view prev_value,
                                       bool had_prev) {
  return AppendLog(LogRecord::LsmPutLogRecord(prev_lsn_, txn_id_, root, key,
                                              value, prev_value, had_prev));
}

StatusOr<lsn_t> Transaction::LsmDeleteLog(page_id_t root, std::string_view key,
                                          std::string_view prev_value) {
  return AppendLog(
      LogRecord::LsmDeleteLogRecord(prev_lsn_, txn_id_, root, key, prev_value));
}

StatusOr<lsn_t> Transaction::LsmDestroyLog(page_id_t root) {
  return AppendLog(LogRecord::LsmDestroyLogRecord(prev_lsn_, txn_id_, root));
}

void Transaction::ReserveIndexUndoBytes(page_id_t pid, size_t bytes) {
  if (bytes == 0 || transaction_manager_ == nullptr) {
    return;
  }
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_INDEX_RESERVATION") != nullptr;
  if (disabled) {
    return;
  }
  transaction_manager_->ReserveIndexUndoBytes(*this, pid, bytes);
}

StatusOr<lsn_t> Transaction::AllocatePageLog(page_id_t allocated_page_id,
                                             PageType new_page_type) {
  return AppendLog(LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type));
}

StatusOr<lsn_t> Transaction::DestroyPageLog(
    page_id_t destroyed_page_id, PageType /*old_page_type*/,
    std::string_view /*old_page_body*/) {
  return AppendLog(
      LogRecord::DestroyPageLogRecord(prev_lsn_, txn_id_, destroyed_page_id));
}

void Transaction::NoteWaitForHolder(txn_id_t holder, RowPosition row) const {
  if (transaction_manager_ == nullptr || holder == 0 || holder == txn_id_) {
    return;
  }
  if (transaction_manager_->GetDeadlockPolicy() ==
      TransactionManager::DeadlockPolicy::kDeadlockDetect) {
    transaction_manager_->AddWaitForEdge(txn_id_, holder, row);
  }
}

void Transaction::ClearWaitForHolder() const {
  if (transaction_manager_ == nullptr) {
    return;
  }
  if (transaction_manager_->GetDeadlockPolicy() ==
      TransactionManager::DeadlockPolicy::kDeadlockDetect) {
    transaction_manager_->RemoveWaitForEdge(txn_id_);
  }
}

void Transaction::CommitWait() const {
  while (transaction_manager_->CommittedLSN() < prev_lsn_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace tinylamb
