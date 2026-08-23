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
#include <atomic>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <iterator>
#include <mutex>
#include <ranges>
#include <string>
#include <optional>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

Transaction TransactionManager::Begin(bool read_only) {
  txn_id_t new_txn_id = next_txn_id_.fetch_add(1);
  Transaction new_txn(new_txn_id, this, read_only);
  if (!read_only) {
    new_txn.prev_lsn_ =
        logger_->AddLog(LogRecord(0, new_txn_id, LogType::kBegin).Serialize());
  }
  {
    std::scoped_lock lk(transaction_table_lock);
    // Snapshot acquisition and registration must be atomic with respect to
    // commit-timestamp publication in CommitVersions (same lock).
    new_txn.snapshot_ts_ = commit_timestamp_.load();
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
  if (!txn.IsReadOnly()) { CommitVersions(txn);
}
  txn.SetStatus(TransactionStatus::kCommitted);
  if (!txn.IsReadOnly()) {
    LogRecord commit_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
    try {
      // The write locks stay held across AddLog/WaitForDurable (durability
      // precedes release), so stretch contenders' wait timeout over any
      // fsync stall instead of letting them die at kExclusiveWaitTimeout.
      LockManager::DurabilityWaitGuard floor_guard(*lock_manager_);
      txn.prev_lsn_ = logger_->AddLog(commit_log.Serialize());
      // AddLog returns the LSN *before* the payload; durable point is end of
      // the buffered commit record.
      const lsn_t commit_end = logger_->BufferedLSN();
      if (synchronous_commit_) {
        logger_->WaitForDurable(commit_end);
      }
    } catch (...) {
      // A dead logger cannot take compensation logs, so full rollback is
      // impossible.  Still leave no half-finished state behind: drop the
      // locks and the registry slot so other transactions are not blocked
      // forever, and report the transaction as aborted, never committed.
      ReleaseLocksAndForget(txn);
      txn.SetStatus(TransactionStatus::kAborted);
      throw;
    }
  }
  ReleaseLocksAndForget(txn);
  return Status::kSuccess;
}

void TransactionManager::Abort(Transaction& txn) {
  if (txn.IsReadOnly()) {
    txn.SetStatus(TransactionStatus::kAborted);
    ReleaseLocksAndForget(txn);
    return;
  }
  // The undo walk holds every write lock, so contenders must not spuriously
  // time out while this transaction flushes its tail or replays undo.
  LockManager::DurabilityWaitGuard floor_guard(*lock_manager_);
  // Iterate prev_lsn to beginning of the transaction with undoing.
  {
    const uint64_t latest_log = txn.prev_lsn_;
    while (CommittedLSN() <= latest_log) {
      // A dead logger never advances CommittedLSN; fall through and let the
      // abort-log write below surface the failure instead of spinning.
      if (logger_->Failed()) { break;
}
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  lsn_t prev = txn.prev_lsn_;
  while (prev != 0) {
    LogRecord lr;
    recovery_->ReadLog(prev, &lr);
    recovery_->LogUndoWithPage(prev, lr, txn.transaction_manager_);
    prev = lr.prev_lsn;
  }
  AbortVersions(txn);
  txn.SetStatus(TransactionStatus::kAborted);
  try {
    LogRecord abort_log(txn.prev_lsn_, txn.txn_id_, LogType::kCommit);
    txn.prev_lsn_ = logger_->AddLog(abort_log.Serialize());
  } catch (...) {
    // Same contract as PreCommit: release locks and leave an aborted state
    // rather than a half-finished transaction blocking everyone.
    ReleaseLocksAndForget(txn);
    throw;
  }
  ReleaseLocksAndForget(txn);
}

StatusOr<std::string> TransactionManager::ReadVersion(
    const Transaction& txn, const RowPosition& rp,
    std::optional<std::string_view> physical) const {
  VersionShard& shard = version_shards_[VersionShardIndex(rp)];
  std::scoped_lock lock(shard.mutex);
  const auto found = shard.versions.find(rp);
  if (found == shard.versions.end()) {
    if (!physical) { return Status::kNotExists;
}
    return std::string(*physical);
  }
  const VersionChain& chain = found->second;
  if (chain.pending && chain.pending->owner == txn.ID()) {
    if (!chain.pending->value) { return Status::kNotExists;
}
    return *chain.pending->value;
  }
  for (const auto& version : std::ranges::reverse_view(chain.committed)) {
    if (version.begin_ts <= txn.SnapshotTimestamp() &&
        txn.SnapshotTimestamp() < version.end_ts) {
      if (!version.value) { return Status::kNotExists;
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
  if (!chain.pending) {
    chain.pending = PendingVersion{.owner=txn.ID(), .value=std::nullopt};
    if (txn.write_set_.size() == 1) {
      pending_txn_count_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  assert(chain.pending->owner == txn.ID());
  chain.pending->value = std::move(after_copy);
}

bool TransactionManager::IndexKeysMayBeStale(const Transaction& txn) const {
  // O(1) IndexScan plan gate: only committed index mutations can hide keys.
  // Concurrent pending writers are resolved per row via ReadVersion.
  return txn.SnapshotTimestamp() <
         max_committed_begin_ts_.load(std::memory_order_acquire);
}

bool TransactionManager::RequiresHistoricalRead(const Transaction& txn) const {
  return IndexKeysMayBeStale(txn);
}

void TransactionManager::CommitVersions(Transaction& txn) {
  // Publish a commit timestamp and all of its row versions atomically with
  // snapshot acquisition in Begin() (transaction_table_lock).
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
  std::scoped_lock transaction_guard(transaction_table_lock);
  const uint64_t commit_ts = commit_timestamp_.fetch_add(1) + 1;
  max_committed_begin_ts_.store(
      std::max(max_committed_begin_ts_.load(std::memory_order_relaxed),
               commit_ts),
      std::memory_order_release);
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
    if (!chain.committed.empty()) { chain.committed.back().end_ts = commit_ts;
}
    chain.committed.push_back({commit_ts, std::numeric_limits<uint64_t>::max(),
                               std::move(chain.pending->value)});
    chain.pending.reset();
  }
  if (!txn.write_set_.empty()) {
    pending_txn_count_.fetch_sub(1, std::memory_order_relaxed);
  }
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
    }
  }
  if (!txn.write_set_.empty()) {
    pending_txn_count_.fetch_sub(1, std::memory_order_relaxed);
  }
}

void TransactionManager::MoveActiveTransaction(Transaction* from,
                                               Transaction* to) {
  std::scoped_lock lk(transaction_table_lock);
  auto it = active_transactions_.find(from->txn_id_);
  if (it != active_transactions_.end() && it->second == from) {
    it->second = to;
  }
}

void TransactionManager::UnregisterActiveTransaction(Transaction* txn) {
  std::scoped_lock lk(transaction_table_lock);
  auto it = active_transactions_.find(txn->txn_id_);
  if (it != active_transactions_.end() && it->second == txn) {
    active_transactions_.erase(it);
  }
}

void TransactionManager::ReleaseLocksAndForget(Transaction& txn) {
  for (const RowPosition& row : txn.write_set_) {
    lock_manager_->ReleaseExclusiveLock(row, txn.txn_id_);
  }
  {
    std::scoped_lock lk(transaction_table_lock);
    active_transactions_.erase(txn.txn_id_);
    active_snapshots_.erase(txn.txn_id_);
  }
  if (!txn.IsReadOnly()) { GarbageCollectVersions();
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
      if (!oldest_snapshot && !chain.pending) {
        // Keep the latest committed version so later snapshots can still
        // reconstruct the row without a physical page copy.
        if (chain.committed.size() > 1) {
          chain.committed.erase(chain.committed.begin(),
                                std::prev(chain.committed.end()));
        }
        ++chain_iter;
        continue;
      }
      if (oldest_snapshot) {
        while (chain.committed.size() > 1 &&
               chain.committed[1].begin_ts <= *oldest_snapshot) {
          chain.committed.erase(chain.committed.begin());
        }
      }
      ++chain_iter;
    }
  }
}

void TransactionManager::CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot) {
  logger_->AddLog(
      LogRecord::CompensatingInsertLogRecord(txn_id, pid, slot).Serialize());
}
void TransactionManager::CompensateInsertLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key) {
  logger_->AddLog(
      LogRecord::CompensatingInsertLogRecord(txn_id, pid, key).Serialize());
}
void TransactionManager::CompensateInsertBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key) {
  logger_->AddLog(LogRecord::CompensatingInsertBranchLogRecord(txn_id, pid, key)
                      .Serialize());
}

void TransactionManager::CompensateUpdateLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateLogRecord(txn_id, pid, slot, redo)
          .Serialize());
}
void TransactionManager::CompensateUpdateLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateLeafLogRecord(txn_id, pid, key, redo)
          .Serialize());
}
void TransactionManager::CompensateUpdateBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key,
                                                   page_id_t redo) {
  logger_->AddLog(
      LogRecord::CompensatingUpdateBranchLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteLog(txn_id_t txn_id, page_id_t pid,
                                             slot_t slot,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingDeleteLogRecord(txn_id, pid, slot, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteLog(txn_id_t txn_id, page_id_t pid,
                                             std::string_view key,
                                             std::string_view redo) {
  logger_->AddLog(
      LogRecord::CompensatingDeleteLeafLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateDeleteBranchLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   std::string_view key,
                                                   page_id_t redo) {
  logger_->AddLog(
      LogRecord::CompensatingDeleteBranchLogRecord(txn_id, pid, key, redo)
          .Serialize());
}

void TransactionManager::CompensateSetLowestValueLog(txn_id_t txn_id,
                                                     page_id_t pid,
                                                     page_id_t redo) {
  logger_->AddLog(
      LogRecord::CompensateSetLowestValueLogRecord(txn_id, pid, redo)
          .Serialize());
}

void TransactionManager::CompensateSetLowFenceLog(txn_id_t txn_id,
                                                  page_id_t pid,
                                                  const IndexKey& redo) {
  logger_->AddLog(
      LogRecord::CompensateSetLowFenceLogRecord(0, txn_id, pid, redo)
          .Serialize());
}

void TransactionManager::CompensateSetHighFenceLog(txn_id_t txn_id,
                                                   page_id_t pid,
                                                   const IndexKey& redo) {
  logger_->AddLog(
      LogRecord::CompensateSetHighFenceLogRecord(0, txn_id, pid, redo)
          .Serialize());
}

void TransactionManager::CompensateSetFosterLog(txn_id_t txn_id, page_id_t pid,
                                                const FosterPair& foster) {
  logger_->AddLog(
      LogRecord::CompensateSetFosterLogRecord(0, txn_id, pid, foster)
          .Serialize());
}

bool TransactionManager::GetExclusiveLock(const RowPosition& rp,
                                          txn_id_t owner) {
  return lock_manager_->GetExclusiveLock(rp, owner);
}

bool TransactionManager::GetSharedLock(const RowPosition& rp,
                                       txn_id_t owner) {
  return lock_manager_->GetSharedLock(rp, owner);
}

bool TransactionManager::TryUpgradeLock(const RowPosition& rp,
                                        txn_id_t owner) {
  return lock_manager_->TryUpgradeLock(rp, owner);
}

uint64_t TransactionManager::AddLog(const LogRecord& lr) {
  return logger_->AddLog(lr.Serialize());
}

uint64_t TransactionManager::CommittedLSN() const {
  return logger_->CommittedLSN();
}

}  // namespace tinylamb
