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
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
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

Transaction::~Transaction() {
  // Destroyed without commit/abort (e.g. an exception unwound a query after
  // Begin): leaving the slot registered hands a dangling Transaction* to the
  // checkpoint worker and the deadlock detector (use-after-free), and pins
  // active_snapshots_ so MVCC GC can never advance.  ForgetTransaction on the
  // normal paths already erased the entry, making this a no-op there.
  if (transaction_manager_ != nullptr && !IsFinished()) {
    transaction_manager_->ReleaseActiveTransaction(this);
  }
}

Transaction::Transaction(txn_id_t txn_id, TransactionManager* tm,
                         bool read_only)
    : txn_id_(txn_id),
      snapshot_ts_(tm->CurrentCommitTimestamp()),
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
      mutated_index_roots_(std::move(o.mutated_index_roots_)),
      shard_epoch_(o.shard_epoch_),
      write_epoch_(o.write_epoch_.load(std::memory_order_acquire)),
      version_read_caches_(std::move(o.version_read_caches_)),
      read_state_mutex_(std::move(o.read_state_mutex_)),
      prev_lsn_(o.prev_lsn_.load(std::memory_order_relaxed)),
      prev_end_lsn_(o.prev_end_lsn_.load(std::memory_order_relaxed)),
      durability_dependence_(
          o.durability_dependence_.load(std::memory_order_acquire)),
      status_(o.status_.load(std::memory_order_relaxed)),
      read_only_(o.read_only_),
      transaction_manager_(o.transaction_manager_),
      wounded_(o.wounded_.load(std::memory_order_acquire)) {
  if (o.transaction_manager_ != nullptr) {
    o.transaction_manager_->MoveActiveTransaction(&o, this);
  }
  // Neutralize the moved-from object: it no longer owns a registry slot or
  // manager services (mirrors PageRef's move convention).
  o.transaction_manager_ = nullptr;
}

Status Transaction::PreCommit() {
  Status result = transaction_manager_->PreCommit(*this);
  if (result == Status::kSuccess) {
    status_ = TransactionStatus::kCommitted;
  } else {
    // A failed PreCommit must not be reported as committed.
    status_ = TransactionStatus::kAborted;
  }
  return result;
}

Status Transaction::Abort() { return transaction_manager_->Abort(*this); }

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
  if (read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    return true;
  }
  if (!transaction_manager_->AcquireWriteIntent(*this, rp, true)) {
    return false;
  }
  // Do not remember a write until its MVCC intent has been reserved.
  write_set_.insert(rp);
  return true;
}

bool Transaction::AddWriteSet(const RowPosition& rp, std::string_view before) {
  assert(!IsFinished());
  if (read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    return true;
  }
  if (!transaction_manager_->AcquireWriteIntent(*this, rp, true, before)) {
    return false;
  }
  write_set_.insert(rp);
  return true;
}

bool Transaction::TryAddWriteSet(const RowPosition& rp) {
  assert(!IsFinished());
  if (read_only_) {
    return false;
  }
  if (write_set_.contains(rp)) {
    return true;
  }
  if (!transaction_manager_->AcquireWriteIntent(*this, rp, false)) {
    return false;
  }
  write_set_.insert(rp);
  return true;
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
    const RowPosition& rp, std::optional<std::string_view> physical) {
  assert(!IsFinished());
  // Fast path: a row without a version chain has exactly one version -- the
  // physical image inside the caller's pinned page -- which every snapshot
  // sees as-is.  Return it directly: no copy, no cache entry, and no locks.
  if (transaction_manager_ != nullptr && physical &&
      !transaction_manager_->HasVersionChain(rp)) {
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
  StatusOr<std::string> visible =
      transaction_manager_->ReadVersion(*this, rp, physical);
  if (!visible.HasValue()) {
    return visible.GetStatus();
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
  auto [iter, inserted] = shard.entries.insert_or_assign(
      rp,
      VersionCacheEntry{.epoch = epoch, .value = std::move(visible.Value())});
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
  const std::string data = lr.Serialize();
  ASSIGN_OR_RETURN(lsn_t, start, transaction_manager_->logger_->AddLog(data));
  prev_lsn_ = start;
  // AddLog returns the record's START offset; the record occupies
  // [start, start + data.size()).  Page stamps use the END so the write-back
  // durability gate cannot pass while the modifying record is still volatile.
  prev_end_lsn_ = start + static_cast<lsn_t>(data.size());
  // Known narrow window: a checkpoint's ATT snapshot can read a stale
  // prev_lsn_ for the few instructions before the store lands.  Recovery
  // tolerates it: any record at or above checkpoint begin_lsn is found by
  // the scan itself, and an older never-durable record is dropped by the
  // torn-tail truncation, so the stale snapshot alone cannot strand an
  // uncompensated durable record below begin_lsn.
  return start;
}

StatusOr<lsn_t> Transaction::InsertLog(page_id_t pid, slot_t slot,
                                       std::string_view redo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pid, slot, redo));
}
StatusOr<lsn_t> Transaction::InsertLeafLog(page_id_t pid, std::string_view key,
                                           std::string_view redo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::InsertingLeafLogRecord(prev_lsn_, txn_id_, pid, key, redo));
}
StatusOr<lsn_t> Transaction::InsertBranchLog(page_id_t pid,
                                             std::string_view key,
                                             page_id_t redo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::InsertingBranchLogRecord(prev_lsn_, txn_id_, pid, key, redo));
}

StatusOr<lsn_t> Transaction::UpdateLog(page_id_t pid, slot_t slot,
                                       std::string_view redo,
                                       std::string_view undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pid, slot, redo, undo));
}

StatusOr<lsn_t> Transaction::UpdateLeafLog(page_id_t pid, std::string_view key,
                                           std::string_view redo,
                                           std::string_view undo) {
  assert(!IsFinished());
  return AppendLog(LogRecord::UpdatingLeafLogRecord(prev_lsn_, txn_id_, pid,
                                                    key, redo, undo));
}

StatusOr<lsn_t> Transaction::UpdateBranchLog(page_id_t pid,
                                             std::string_view key,
                                             page_id_t redo, page_id_t undo) {
  assert(!IsFinished());
  return AppendLog(LogRecord::UpdatingBranchLogRecord(prev_lsn_, txn_id_, pid,
                                                      key, redo, undo));
}

StatusOr<lsn_t> Transaction::DeleteLog(page_id_t pid, slot_t slot,
                                       std::string_view undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pid, slot, undo));
}

StatusOr<lsn_t> Transaction::DeleteLeafLog(page_id_t pid, std::string_view key,
                                           std::string_view undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::DeletingLeafLogRecord(prev_lsn_, txn_id_, pid, key, undo));
}

StatusOr<lsn_t> Transaction::DeleteBranchLog(page_id_t pid,
                                             std::string_view key,
                                             page_id_t undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::DeletingBranchLogRecord(prev_lsn_, txn_id_, pid, key, undo));
}

StatusOr<lsn_t> Transaction::SetLowestLog(page_id_t pid, page_id_t redo,
                                          page_id_t undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::SetLowestLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::SetLowFence(page_id_t pid, const IndexKey& redo,
                                         const IndexKey& undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::SetLowFenceLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::SetHighFence(page_id_t pid, const IndexKey& redo,
                                          const IndexKey& undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::SetHighFenceLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::SetFoster(page_id_t pid, const FosterPair& redo,
                                       const FosterPair& undo) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::SetFosterLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
}

StatusOr<lsn_t> Transaction::AllocatePageLog(page_id_t allocated_page_id,
                                             PageType new_page_type) {
  assert(!IsFinished());
  return AppendLog(LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type));
}

StatusOr<lsn_t> Transaction::DestroyPageLog(page_id_t destroyed_page_id,
                                            PageType old_page_type,
                                            std::string old_page_body) {
  assert(!IsFinished());
  return AppendLog(
      LogRecord::DestroyPageLogRecord(prev_lsn_, txn_id_, destroyed_page_id,
                                      old_page_type, std::move(old_page_body)));
}

// Using this function is discouraged to get performance of flush pipelining.
void Transaction::CommitWait() const {
  while (transaction_manager_->CommittedLSN() < prev_lsn_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace tinylamb
