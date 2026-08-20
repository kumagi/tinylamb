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

#include <cassert>
#include <chrono>
#include <iostream>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
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

Transaction::Transaction(txn_id_t txn_id, TransactionManager* tm,
                         bool read_only)
    : txn_id_(txn_id),
      snapshot_ts_(tm->CurrentCommitTimestamp()),
      status_(TransactionStatus::kRunning),
      read_only_(read_only),
      transaction_manager_(tm) {}

Status Transaction::PreCommit() {
  Status result = transaction_manager_->PreCommit(*this);
  status_ = TransactionStatus::kCommitted;
  return result;
}

void Transaction::Abort() { transaction_manager_->Abort(*this); }

void Transaction::SetStatus(TransactionStatus status) { status_ = status; }

bool Transaction::AddReadSet(const RowPosition& rp) {
  assert(!IsFinished());
  // MV2PL readers use snapshot-visible row versions and therefore never take
  // a lock that conflicts with a writer's exclusive lock.
  read_set_.insert(rp);
  return true;
}

bool Transaction::AddWriteSet(const RowPosition& rp) {
  assert(!IsFinished());
  if (read_only_) return false;
  if (write_set_.find(rp) != write_set_.end()) {
    return true;
  }
  if (!transaction_manager_->GetExclusiveLock(rp)) {
    return false;
  }
  // Do not remember a write lock until it has actually been acquired.
  write_set_.insert(rp);
  return true;
}

StatusOr<std::string_view> Transaction::ReadVersion(
    const RowPosition& rp, std::optional<std::string_view> physical) {
  assert(!IsFinished());
  std::scoped_lock state_guard(*read_state_mutex_);
  AddReadSet(rp);
  if (!transaction_manager_) {
    if (!physical) return Status::kNotExists;
    return *physical;
  }
  StatusOr<std::string> visible =
      transaction_manager_->ReadVersion(*this, rp, physical);
  if (!visible.HasValue()) return visible.GetStatus();
  auto [iter, inserted] =
      version_read_cache_.insert_or_assign(rp, std::move(visible.Value()));
  constexpr size_t kMaxVersionReadCache = 4096;
  if (version_read_cache_.size() > kMaxVersionReadCache) {
    std::string keep = std::move(iter->second);
    version_read_cache_.clear();
    iter = version_read_cache_.insert_or_assign(rp, std::move(keep)).first;
  }
  return std::string_view(iter->second);
}

void Transaction::RegisterVersionWrite(const RowPosition& rp,
                                       std::optional<std::string_view> before,
                                       std::optional<std::string_view> after) {
  std::scoped_lock state_guard(*read_state_mutex_);
  if (!transaction_manager_) return;
  transaction_manager_->RegisterVersionWrite(*this, rp, before, after);
  version_read_cache_.erase(rp);
}

bool Transaction::RequiresHistoricalRead() const {
  return transaction_manager_ &&
         transaction_manager_->RequiresHistoricalRead(*this);
}

bool Transaction::IndexKeysMayBeStale() const {
  return transaction_manager_ &&
         transaction_manager_->IndexKeysMayBeStale(*this);
}

lsn_t Transaction::InsertLog(page_id_t pid, slot_t slot,
                             std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::InsertingLogRecord(prev_lsn_, txn_id_, pid, slot, redo));
  return prev_lsn_;
}
lsn_t Transaction::InsertLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::InsertingLeafLogRecord(prev_lsn_, txn_id_, pid, key, redo));
  return prev_lsn_;
}
lsn_t Transaction::InsertBranchLog(page_id_t pid, std::string_view key,
                                   page_id_t redo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::InsertingBranchLogRecord(prev_lsn_, txn_id_, pid, key, redo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateLog(page_id_t pid, slot_t slot, std::string_view redo,
                             std::string_view undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::UpdatingLogRecord(prev_lsn_, txn_id_, pid, slot, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view redo, std::string_view undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(LogRecord::UpdatingLeafLogRecord(
      prev_lsn_, txn_id_, pid, key, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::UpdateBranchLog(page_id_t pid, std::string_view key,
                                   page_id_t redo, page_id_t undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(LogRecord::UpdatingBranchLogRecord(
      prev_lsn_, txn_id_, pid, key, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::DeleteLog(page_id_t pid, slot_t slot,
                             std::string_view undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::DeletingLogRecord(prev_lsn_, txn_id_, pid, slot, undo));
  return prev_lsn_;
}

lsn_t Transaction::DeleteLeafLog(page_id_t pid, std::string_view key,
                                 std::string_view undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::DeletingLeafLogRecord(prev_lsn_, txn_id_, pid, key, undo));
  return prev_lsn_;
}

lsn_t Transaction::DeleteBranchLog(page_id_t pid, std::string_view key,
                                   page_id_t undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::DeletingBranchLogRecord(prev_lsn_, txn_id_, pid, key, undo));
  return prev_lsn_;
}

lsn_t Transaction::SetLowestLog(page_id_t pid, page_id_t redo, page_id_t undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::SetLowestLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::SetLowFence(page_id_t pid, const IndexKey& redo,
                               const IndexKey& undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::SetLowFenceLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::SetHighFence(page_id_t pid, const IndexKey& redo,
                                const IndexKey& undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::SetHighFenceLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::SetFoster(page_id_t pid, const FosterPair& redo,
                             const FosterPair& undo) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::SetFosterLogRecord(prev_lsn_, txn_id_, pid, redo, undo));
  return prev_lsn_;
}

lsn_t Transaction::AllocatePageLog(page_id_t allocated_page_id,
                                   PageType new_page_type) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(LogRecord::AllocatePageLogRecord(
      prev_lsn_, txn_id_, allocated_page_id, new_page_type));
  return prev_lsn_;
}

lsn_t Transaction::DestroyPageLog(page_id_t destroyed_page_id) {
  assert(!IsFinished());
  prev_lsn_ = transaction_manager_->AddLog(
      LogRecord::DestroyPageLogRecord(prev_lsn_, txn_id_, destroyed_page_id));
  return prev_lsn_;
}

// Using this function is discouraged to get performance of flush pipelining.
void Transaction::CommitWait() const {
  while (transaction_manager_->CommittedLSN() < prev_lsn_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

}  // namespace tinylamb
