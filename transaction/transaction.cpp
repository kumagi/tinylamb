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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "common/serdes.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "common/unwind_guard.hpp"
#include "page/page_manager.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

TransactionWriteSet::TransactionWriteSet() = default;

TransactionWriteSet::~TransactionWriteSet() { clear(); }

TransactionWriteSet::TransactionWriteSet(TransactionWriteSet&& o) noexcept
    : small_(std::move(o.small_)),
      spill_(std::move(o.spill_)),
      spill_tail_(std::move(o.spill_tail_)),
      spill_store_(std::move(o.spill_store_)),
      segment_writer_(std::move(o.segment_writer_)),
      spill_segment_id_(o.spill_segment_id_),
      mem_charge_(std::move(o.mem_charge_)),
      total_count_(o.total_count_),
      spilled_(o.spilled_),
      disk_spilled_(o.disk_spilled_) {
  o.spill_segment_id_ = kInvalidSpillSegmentId;
  o.total_count_ = 0;
  o.spilled_ = false;
  o.disk_spilled_ = false;
}

TransactionWriteSet& TransactionWriteSet::operator=(
    TransactionWriteSet&& o) noexcept {
  if (this == &o) {
    return *this;
  }
  clear();
  small_ = std::move(o.small_);
  spill_ = std::move(o.spill_);
  spill_tail_ = std::move(o.spill_tail_);
  spill_store_ = std::move(o.spill_store_);
  segment_writer_ = std::move(o.segment_writer_);
  spill_segment_id_ = o.spill_segment_id_;
  mem_charge_ = std::move(o.mem_charge_);
  total_count_ = o.total_count_;
  spilled_ = o.spilled_;
  disk_spilled_ = o.disk_spilled_;
  o.spill_segment_id_ = kInvalidSpillSegmentId;
  o.total_count_ = 0;
  o.spilled_ = false;
  o.disk_spilled_ = false;
  return *this;
}

Status TransactionWriteSet::FlushTailToWriter(uint64_t* spilled_bytes_out) {
  if (spill_tail_.empty() || segment_writer_ == nullptr) {
    return Status::kSuccess;
  }
  const size_t bytes = spill_tail_.size() * sizeof(RowPosition);
  RETURN_IF_FAIL(segment_writer_->Write(spill_tail_.data(), bytes));
  spill_tail_.clear();
  if (spilled_bytes_out != nullptr) {
    *spilled_bytes_out += bytes;
  }
  return Status::kSuccess;
}

bool TransactionWriteSet::TryInsert(const value_type& value,
                                    const std::shared_ptr<SpillStore>& store,
                                    size_t max_in_memory_entries,
                                    uint64_t* spilled_bytes_out,
                                    bool* initial_spill_out,
                                    Status* error_out) {
  if (spilled_bytes_out != nullptr) {
    *spilled_bytes_out = 0;
  }
  if (initial_spill_out != nullptr) {
    *initial_spill_out = false;
  }
  if (error_out != nullptr) {
    *error_out = Status::kSuccess;
  }
  if (disk_spilled_) {
    if (spill_tail_.size() >= kSpillTailCapacity) {
      const Status flush_st = FlushTailToWriter(spilled_bytes_out);
      if (flush_st != Status::kSuccess) {
        if (error_out != nullptr) {
          *error_out = flush_st;
        }
        return false;
      }
    }
    spill_tail_.push_back(value);
    ++total_count_;
    return true;
  }

  auto transition_to_disk = [&](const auto& entries) -> bool {
    if (store == nullptr) {
      if (error_out != nullptr) {
        *error_out = StatusError(
            StatusCode::kMemoryLimitExceeded,
            "transaction write-set/version payload exceeds the query memory "
            "limit and no spill store is configured (pool=query)");
      }
      return false;
    }
    // Reserve the fixed spill-buffer charge before any destructive step. The
    // buffer is allocated unconditionally below, so its charge is mandatory;
    // if the reservation fails we must still own small_/spill_ in full. A
    // return here with the in-memory entries already swapped away leaves the
    // write set empty while commit/abort release write intents by iterating it
    // (write_set_.ForEach), pinning every one of those intents to this txn
    // forever. Use a temporary charge so a failed TryAdd cannot disturb the
    // entry charges held by mem_charge_.
    ProcessMemoryCharge spill_charge(MemoryPoolKind::kTx, 0);
    if (!spill_charge.TryAdd(
            kSpillBufferBytes + (kSpillTailCapacity * sizeof(RowPosition)),
            /*respect_soft_limit=*/false)) {
      if (error_out != nullptr) {
        *error_out = StatusError(
            StatusCode::kMemoryLimitExceeded,
            "cannot reserve transaction write-set spill buffer charge");
      }
      return false;
    }
    std::vector<RowPosition> chunk;
    try {
      chunk.reserve(kSpillTailCapacity);
    } catch (const std::bad_alloc&) {
      if (error_out != nullptr) {
        *error_out = Status::kMemoryLimitExceeded;
      }
      return false;
    }
    try {
      segment_writer_ =
          std::make_unique<SpillSegmentWriter>(store.get(), kSpillBufferBytes);
    } catch (const std::bad_alloc&) {
      if (error_out != nullptr) {
        *error_out = Status::kMemoryLimitExceeded;
      }
      return false;
    }
    const Status open_st = segment_writer_->EnsureOpen();
    if (open_st != Status::kSuccess) {
      if (error_out != nullptr) {
        *error_out = open_st;
      }
      segment_writer_.reset();
      return false;
    }
    spill_store_ = store;
    spill_segment_id_ = segment_writer_->SegmentId();
    uint64_t written = 0;
    for (const RowPosition& rp : entries) {
      chunk.push_back(rp);
      if (chunk.size() >= kSpillTailCapacity) {
        const size_t bytes = chunk.size() * sizeof(RowPosition);
        const Status write_st = segment_writer_->Write(chunk.data(), bytes);
        if (write_st != Status::kSuccess) {
          if (error_out != nullptr) {
            *error_out = write_st;
          }
          segment_writer_
              ->Abort();  // NOLINT(bugprone-unused-return-value): best-effort
                          // rollback preserves the primary error.
          segment_writer_.reset();
          spill_segment_id_ = kInvalidSpillSegmentId;
          spill_store_.reset();
          return false;
        }
        written += bytes;
        chunk.clear();
      }
    }
    if (!chunk.empty()) {
      const size_t bytes = chunk.size() * sizeof(RowPosition);
      const Status write_st = segment_writer_->Write(chunk.data(), bytes);
      if (write_st != Status::kSuccess) {
        if (error_out != nullptr) {
          *error_out = write_st;
        }
        segment_writer_
            ->Abort();  // NOLINT(bugprone-unused-return-value): best-effort
                        // rollback preserves the primary error.
        segment_writer_.reset();
        spill_segment_id_ = kInvalidSpillSegmentId;
        spill_store_.reset();
        return false;
      }
      written += bytes;
    }
    std::vector<RowPosition>().swap(small_);
    std::unordered_set<RowPosition>().swap(spill_);
    // Move assignment releases the now-obsolete entry charges and takes over
    // the already-reserved spill-buffer charge in one step.
    mem_charge_ = std::move(spill_charge);
    spilled_ = false;
    disk_spilled_ = true;
    if (initial_spill_out != nullptr) {
      *initial_spill_out = true;
    }
    if (spilled_bytes_out != nullptr) {
      *spilled_bytes_out += written;
    }
    chunk.clear();
    chunk.push_back(value);
    spill_tail_.swap(chunk);
    ++total_count_;
    return true;
  };

  if (spilled_) {
    if (spill_.contains(value)) {
      return true;
    }
    const bool over_entries = spill_.size() >= max_in_memory_entries;
    const bool charge_ok =
        !over_entries &&
        mem_charge_.TryAdd(kEntryChargeBytes, /*respect_soft_limit=*/false);
    if ((over_entries || !charge_ok) && store != nullptr) {
      return transition_to_disk(spill_);
    }
    if (!charge_ok && store == nullptr) {
      if (error_out != nullptr) {
        *error_out = StatusError(
            StatusCode::kMemoryLimitExceeded,
            "transaction write-set/version payload exceeds the query memory "
            "limit and no spill store is configured (pool=query)");
      }
      return false;
    }
    spill_.insert(value);
    ++total_count_;
    return true;
  }

  const auto position =
      std::lower_bound(small_.begin(), small_.end(), value, Less{});
  if (position != small_.end() && *position == value) {
    return true;
  }
  const size_t small_cap =
      std::min(kSmallCapacity, std::max<size_t>(1, max_in_memory_entries));
  if (small_.size() < small_cap) {
    small_.insert(position, value);
    ++total_count_;
    return true;
  }
  if (small_.size() >= max_in_memory_entries && store != nullptr) {
    return transition_to_disk(small_);
  }
  const size_t need_bytes = (small_.size() + 1) * kEntryChargeBytes;
  if (!mem_charge_.TryAdd(need_bytes, /*respect_soft_limit=*/false)) {
    if (store != nullptr) {
      return transition_to_disk(small_);
    }
    if (error_out != nullptr) {
      *error_out = StatusError(
          StatusCode::kMemoryLimitExceeded,
          "transaction write-set/version payload exceeds the query memory "
          "limit and no spill store is configured (pool=query)");
    }
    return false;
  }
  std::unordered_set<RowPosition> replacement;
  replacement.reserve(small_.size() + 1);
  for (const RowPosition& entry : small_) {
    replacement.insert(entry);
  }
  replacement.insert(value);
  spill_.swap(replacement);
  std::vector<RowPosition>().swap(small_);
  spilled_ = true;
  ++total_count_;
  return true;
}

TransactionWriteSet::size_type TransactionWriteSet::erase(
    const value_type& value) {
  if (disk_spilled_) {
    const size_t n = std::erase(spill_tail_, value);
    if (n > 0) {
      total_count_ = total_count_ >= n ? total_count_ - n : 0;
      return n;
    }
    // The value may already be in a flushed spill segment, which this
    // in-memory erase cannot reach; report 0 instead of lying to the caller.
    return 0;
  }
  if (spilled_) {
    const size_type n = spill_.erase(value);
    if (n > 0 && total_count_ > 0) {
      --total_count_;
      mem_charge_.Release(kEntryChargeBytes);
    }
    return n;
  }
  const auto position =
      std::lower_bound(small_.begin(), small_.end(), value, Less{});
  if (position == small_.end() || *position != value) {
    return 0;
  }
  small_.erase(position);
  if (total_count_ > 0) {
    --total_count_;
  }
  return 1;
}

Status TransactionWriteSet::ForEach(
    const std::function<Status(const RowPosition&)>& fn) {
  if (!disk_spilled_) {
    if (spilled_) {
      for (const RowPosition& rp : spill_) {
        RETURN_IF_FAIL(fn(rp));
      }
    } else {
      for (const RowPosition& rp : small_) {
        RETURN_IF_FAIL(fn(rp));
      }
    }
    return Status::kSuccess;
  }
  for (const RowPosition& rp : spill_tail_) {
    RETURN_IF_FAIL(fn(rp));
  }
  // Keep the tail until Finish succeeds: if the writer fails, a retrying
  // ForEach must see these rows again instead of silently dropping them.
  SpillSegmentId seg_id = spill_segment_id_;
  if (segment_writer_ != nullptr) {
    ASSIGN_OR_RETURN(SpillSegmentId, finished_seg,
                     segment_writer_->Finish(/*keep_empty=*/false));
    seg_id = finished_seg;
    segment_writer_.reset();
    spill_segment_id_ = seg_id;
  }
  // The tail rows were all delivered by the loop above; clearing them here
  // (rather than inside the writer block) keeps a retry from re-delivering
  // them while the segment read below stays retryable through
  // spill_segment_id_.
  std::vector<RowPosition>().swap(spill_tail_);
  if (seg_id != kInvalidSpillSegmentId && spill_store_ != nullptr) {
    const auto store = spill_store_;
    SpillSegmentReader reader(store.get(), seg_id, kSpillBufferBytes);
    // The segment is dropped only after every row was delivered.  An early
    // return (Open failure, fn failure, read error) used to drop it through
    // the unwind guard AND clear spill_segment_id_/spill_tail_, so a retrying
    // ForEach silently succeeded with an empty write set and scattered
    // entries were lost.  Keeping both lets the retry re-read the segment;
    // re-delivering already-seen segment rows matches the in-memory path's
    // retry semantics.
    RETURN_IF_FAIL(reader.Open());
    std::array<RowPosition, 512> batch{};
    while (!reader.Eof()) {
      ASSIGN_OR_RETURN(size_t, bytes,
                       reader.ReadUpTo(batch.data(), sizeof(batch)));
      if (bytes == 0) {
        break;
      }
      const size_t count = bytes / sizeof(RowPosition);
      for (size_t i = 0; i < count; ++i) {
        RETURN_IF_FAIL(fn(batch[i]));
      }
    }
    store->DropSegment(seg_id);
    spill_segment_id_ = kInvalidSpillSegmentId;
  }
  clear();
  return Status::kSuccess;
}

void TransactionWriteSet::clear() {
  std::vector<RowPosition>().swap(small_);
  std::unordered_set<RowPosition>().swap(spill_);
  std::vector<RowPosition>().swap(spill_tail_);
  if (segment_writer_ != nullptr) {
    segment_writer_
        ->Abort();  // NOLINT(bugprone-unused-return-value): best-effort
                    // rollback preserves the primary error.
    segment_writer_.reset();
  }
  if (spill_segment_id_ != kInvalidSpillSegmentId && spill_store_ != nullptr) {
    spill_store_->DropSegment(spill_segment_id_);
  }
  spill_segment_id_ = kInvalidSpillSegmentId;
  spill_store_.reset();
  mem_charge_.ReleaseAll();
  total_count_ = 0;
  spilled_ = false;
  disk_spilled_ = false;
}

bool Transaction::FindPendingLsmWrite(page_id_t root, std::string_view key,
                                      std::string* value, bool* deleted) const {
  bool reread = false;
  {
    std::scoped_lock guard(chain_mu_);
    reread = wal_reread_mode_;
    if (!reread) {
      const auto records = std::views::reverse(staged_records_);
      const auto found =
          std::ranges::find_if(records, [&](const LogRecord& record) {
            return record.pid == root && record.key == key &&
                   (record.type == LogType::kLsmPut ||
                    record.type == LogType::kLsmDelete);
          });
      if (found == records.end()) {
        return false;
      }
      *deleted = found->type == LogType::kLsmDelete;
      *value = *deleted ? std::string() : found->redo_data;
      return true;
    }
  }
  return transaction_manager_ != nullptr &&
         transaction_manager_->FindPendingLsmWriteViaWal(*this, root, key,
                                                         value, deleted);
}

void Transaction::ForEachPendingLsmWrite(
    page_id_t root, std::string_view key,
    const std::function<void(const LogRecord&)>& fn) const {
  bool reread = false;
  {
    std::scoped_lock guard(chain_mu_);
    reread = wal_reread_mode_;
    if (!reread) {
      for (const LogRecord& record : staged_records_) {
        if (record.pid == root && record.key == key &&
            (record.type == LogType::kLsmPut ||
             record.type == LogType::kLsmDelete)) {
          fn(record);
        }
      }
      return;
    }
  }
  if (transaction_manager_ != nullptr) {
    transaction_manager_->ForEachPendingLsmWriteViaWal(*this, root, key, fn);
  }
}

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
          const Status aborted = manager->Abort(*this);
          if (!aborted.ok()) {
            LOG(ERROR) << "automatic transaction abort failed: " << aborted;
          }
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
        staged_charge_.ReleaseAll();
        staged_bytes_ = 0;
        wal_reread_mode_ = false;
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

void Transaction::AddFinishHook(std::shared_ptr<TransactionFinishHook> hook) {
  if (hook == nullptr) {
    return;
  }
  {
    std::scoped_lock lock(finish_hooks_mu_);
    if (!finish_hooks_finalized_.load(std::memory_order_relaxed)) {
      finish_hooks_.push_back(std::move(hook));
      return;
    }
  }
  const TransactionStatus status = status_.load(std::memory_order_acquire);
  if (status == TransactionStatus::kCommitted ||
      status == TransactionStatus::kAborted) {
    hook->OnTransactionFinished(status);
  }
}

void Transaction::FinalizeFinishHooks() noexcept {
  if (!IsFinished()) {
    return;
  }
  std::vector<std::shared_ptr<TransactionFinishHook>> hooks;
  {
    std::scoped_lock lock(finish_hooks_mu_);
    if (finish_hooks_finalized_.exchange(true, std::memory_order_relaxed)) {
      return;
    }
    hooks.swap(finish_hooks_);
  }
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

      shard_epoch_(NextShardEpoch()),
      transaction_manager_(tm),
      status_(TransactionStatus::kRunning),
      read_only_(read_only) {}

// Begin() registers the address of the Transaction it is about to return.
// NRVO usually makes that address the final one; when the return instead
// move-constructs (e.g. Database::BeginContext moves the result into
// TransactionContext::txn_), follow the registration so the manager's
// active_transactions_ map never points at the moved-from object.
Transaction::Transaction(Transaction&& o) noexcept
    : txn_id_(o.txn_id_),
      snapshot_ts_(o.snapshot_ts_),
      read_set_count_(o.read_set_count_),
      staged_bytes_(o.staged_bytes_),
      shard_epoch_(o.shard_epoch_),
      write_epoch_(o.write_epoch_.load(std::memory_order_acquire)),
      read_state_mutex_(std::move(o.read_state_mutex_)),
      // chain_mu_ is not in the init list on purpose: it stays per-object.
      prev_lsn_(o.prev_lsn_.load(std::memory_order_relaxed)),
      prev_end_lsn_(o.prev_end_lsn_.load(std::memory_order_relaxed)),
      first_lsn_(o.first_lsn_.load(std::memory_order_relaxed)),
      durability_dependence_(
          o.durability_dependence_.load(std::memory_order_acquire)),
      transaction_manager_(o.transaction_manager_),
      statement_timeout_ms_(
          o.statement_timeout_ms_.load(std::memory_order_relaxed)),
      statement_deadline_ns_(
          o.statement_deadline_ns_.load(std::memory_order_relaxed)),
      recycled_page_allocations_(std::move(o.recycled_page_allocations_)),
      mutated_index_roots_(std::move(o.mutated_index_roots_)),
      index_undo_reservations_(std::move(o.index_undo_reservations_)),
      staged_records_(std::move(o.staged_records_)),
      staged_charge_(std::move(o.staged_charge_)),
      finish_hooks_(std::move(o.finish_hooks_)),
      serialization_scratch_(std::move(o.serialization_scratch_)),
      read_set_(std::move(o.read_set_)),
      version_read_caches_(std::move(o.version_read_caches_)),
      write_set_(std::move(o.write_set_)),
      write_set_spill_store_(std::move(o.write_set_spill_store_)),
      wal_reread_mode_(o.wal_reread_mode_),
      has_log_(o.has_log_.load(std::memory_order_relaxed)),
      has_table_drop_(o.has_table_drop_.load(std::memory_order_acquire)),
      finish_hooks_finalized_(
          o.finish_hooks_finalized_.load(std::memory_order_acquire)),
      status_(o.status_.load(std::memory_order_relaxed)),
      failed_(o.failed_.load(std::memory_order_acquire)),
      read_only_(o.read_only_),
      wounded_(o.wounded_.load(std::memory_order_acquire)),
      abort_started_(o.abort_started_.load(std::memory_order_acquire)),
      catalog_dirty_(o.catalog_dirty_),
      intent_wait_failure_(o.intent_wait_failure_),
      write_set_failure_status_(std::move(o.write_set_failure_status_)) {
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
  o.wal_reread_mode_ = false;
  o.staged_bytes_ = 0;
  o.read_set_count_ = 0;
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
    Status abort_status = Status::kRuntimeError;
    try {
      abort_status = Abort();
    } catch (...) {
      failed_.store(true, std::memory_order_release);
    }
    FinishRunningNoexcept();
    return abort_status != Status::kSuccess ? abort_status
                                            : Status::kRuntimeError;
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
    staged_charge_.ReleaseAll();
    staged_bytes_ = 0;
    wal_reread_mode_ = false;
    transaction_manager_->AbortVersions(*this);
    status_.store(TransactionStatus::kAborted, std::memory_order_release);
    transaction_manager_->ForgetTransaction(*this);
    FinalizeFinishHooks();
    return HasLogRecord() ? Status::kRuntimeError : Status::kSuccess;
  }
  return transaction_manager_->Abort(*this);
}

bool Transaction::AddReadSet(const RowPosition& rp) {
  if (IsFinished()) {
    return false;
  }
  // MV2PL readers use snapshot-visible row versions and therefore never take
  // a lock that conflicts with a writer's exclusive lock.
  // Keep a bounded diagnostic sample in RAM so large scans never blow kTx.
  std::scoped_lock state_guard(*read_state_mutex_);
  ++read_set_count_;
  if (read_set_.size() < kMaxDiagnosticReadSetEntries) {
    read_set_.insert(rp);
  }
  return true;
}

bool Transaction::TryInsertWriteSet(const RowPosition& rp, Status* error_out) {
  std::shared_ptr<SpillStore> store;
  // Mirrors TransactionManager::kDefaultWriteSetSpillEntries (4096) when no
  // manager is attached.
  size_t threshold = TransactionManager::kDefaultWriteSetSpillEntries;
  if (transaction_manager_ != nullptr) {
    threshold = transaction_manager_->EffectiveWriteSetSpillThresholdEntries();
    if (!write_set_.IsDiskSpilled()) {
      // P472: the manager's spill store never changes once created, so
      // fetch (and mutex-guard) it at most once per transaction instead of
      // taking the global write_set_spill_mu_ on every insert while the
      // write set sits in the 64..threshold pre-spill window.
      if (write_set_spill_store_ == nullptr &&
          write_set_.size() >= std::min<size_t>(threshold, 64)) {
        write_set_spill_store_ =
            transaction_manager_->GetOrCreateWriteSetSpillStore();
      }
      store = write_set_spill_store_;
    }
  } else {
    // Apply the same memory-budget cap as
    // TransactionManager::EffectiveWriteSetSpillThresholdEntries: a
    // manager-less (embedded/test) Transaction must not hold 4096 entries
    // resident when the process budget would shrink the threshold to 16.
    const size_t pool_max = ProcessMemoryBudget::Global().EffectivePoolMaxBytes(
        MemoryPoolKind::kTx);
    if (pool_max > 0) {
      const size_t by_bytes = std::max<size_t>(16, (pool_max / 8) / 40);
      threshold = std::min(threshold, by_bytes);
    }
  }
  uint64_t spilled_bytes = 0;
  bool initial_spill = false;
  const bool ok = write_set_.TryInsert(rp, store, threshold, &spilled_bytes,
                                       &initial_spill, error_out);
  if (transaction_manager_ != nullptr && (initial_spill || spilled_bytes > 0)) {
    transaction_manager_->RecordWriteSetSpill(spilled_bytes, initial_spill);
  }
  return ok;
}

bool Transaction::AddWriteSet(const RowPosition& rp) {
  write_set_failure_status_ = Status::kSuccess;
  // IsFinished() must be enforced, not just asserted: a stale caller that
  // keeps writing through a committed transaction would install a permanent
  // write intent that no PreCommit/Abort can ever release (both short-circuit
  // on a finished status), blocking that row for the life of the database.
  if (IsFailed()) {
    RecordWriteSetFailure(Status::kRuntimeError);
    return false;
  }
  if (IsFinished() || read_only_ || transaction_manager_ == nullptr) {
    return false;
  }
  if (IsWounded()) {
    RecordIntentWaitFailure(IntentWaitFailure::kWounded);
    return false;
  }
  if (transaction_manager_->FailStopped()) {
    RecordIntentWaitFailure(IntentWaitFailure::kFailStopped);
    RecordWriteSetFailure(StatusError(StatusCode::kIOError,
                                      "transaction manager is fail-stopped"));
    return false;
  }
  if (HasWriteSet(rp)) {
    return true;
  }
  {
    UnwindGuard rollback(
        [this, &rp] { transaction_manager_->ReleaseWriteIntent(*this, rp); });
    if (!transaction_manager_->AcquireWriteIntent(*this, rp, true)) {
      rollback.Disarm();
      return false;
    }
    Status insert_err = Status::kSuccess;
    if (!TryInsertWriteSet(rp, &insert_err)) {
      RecordWriteSetFailure(insert_err != Status::kSuccess
                                ? std::move(insert_err)
                                : Status::kMemoryLimitExceeded);
      return false;
    }
    rollback.Disarm();
    return true;
  }
}

bool Transaction::AddWriteSet(const RowPosition& rp, std::string_view before) {
  write_set_failure_status_ = Status::kSuccess;
  if (IsFailed()) {
    RecordWriteSetFailure(Status::kRuntimeError);
    return false;
  }
  if (IsFinished() || read_only_ || transaction_manager_ == nullptr) {
    return false;
  }
  if (IsWounded()) {
    RecordIntentWaitFailure(IntentWaitFailure::kWounded);
    return false;
  }
  if (transaction_manager_->FailStopped()) {
    RecordIntentWaitFailure(IntentWaitFailure::kFailStopped);
    RecordWriteSetFailure(StatusError(StatusCode::kIOError,
                                      "transaction manager is fail-stopped"));
    return false;
  }
  if (HasWriteSet(rp)) {
    // The write set may have pre-reserved this row without a before-image
    // (TryAddWriteSet); re-acquire so the image is recorded and concurrent
    // snapshots keep seeing the pre-update value.
    const bool acquired =
        transaction_manager_->AcquireWriteIntent(*this, rp, true, before);
    if (!acquired) {
      MarkFailed();
    }
    return acquired;
  }
  {
    UnwindGuard rollback(
        [this, &rp] { transaction_manager_->ReleaseWriteIntent(*this, rp); });
    if (!transaction_manager_->AcquireWriteIntent(*this, rp, true, before)) {
      rollback.Disarm();
      return false;
    }
    Status insert_err = Status::kSuccess;
    if (!TryInsertWriteSet(rp, &insert_err)) {
      RecordWriteSetFailure(insert_err != Status::kSuccess
                                ? std::move(insert_err)
                                : Status::kMemoryLimitExceeded);
      return false;
    }
    rollback.Disarm();
    return true;
  }
}

bool Transaction::TryAddWriteSet(const RowPosition& rp) {
  write_set_failure_status_ = Status::kSuccess;
  if (IsFailed()) {
    RecordWriteSetFailure(Status::kRuntimeError);
    return false;
  }
  if (IsFinished() || read_only_ || transaction_manager_ == nullptr) {
    return false;
  }
  if (IsWounded()) {
    RecordIntentWaitFailure(IntentWaitFailure::kWounded);
    return false;
  }
  if (transaction_manager_->FailStopped()) {
    RecordIntentWaitFailure(IntentWaitFailure::kFailStopped);
    RecordWriteSetFailure(StatusError(StatusCode::kIOError,
                                      "transaction manager is fail-stopped"));
    return false;
  }
  if (HasWriteSet(rp)) {
    return true;
  }
  {
    UnwindGuard rollback(
        [this, &rp] { transaction_manager_->ReleaseWriteIntent(*this, rp); });
    if (!transaction_manager_->AcquireWriteIntent(*this, rp, false)) {
      rollback.Disarm();
      return false;
    }
    Status insert_err = Status::kSuccess;
    if (!TryInsertWriteSet(rp, &insert_err)) {
      RecordWriteSetFailure(insert_err != Status::kSuccess
                                ? std::move(insert_err)
                                : Status::kMemoryLimitExceeded);
      return false;
    }
    rollback.Disarm();
    return true;
  }
}

bool Transaction::WriteIntentUnavailable(const RowPosition& rp) const {
  if (IsFailed() || IsFinished() || read_only_ ||
      transaction_manager_ == nullptr || IsWounded() ||
      transaction_manager_->FailStopped()) {
    return true;
  }
  if (HasWriteSet(rp)) {
    return false;
  }
  return transaction_manager_->HasForeignWriteIntent(*this, rp);
}

bool Transaction::SnapshotSeesRow(const RowPosition& rp) const {
  return transaction_manager_ != nullptr &&
         transaction_manager_->SnapshotSeesRow(*this, rp);
}

void Transaction::ReleaseWriteIntent(const RowPosition& rp) {
  if (transaction_manager_ == nullptr) {
    write_set_.erase(rp);
    return;
  }
  if (!transaction_manager_->ReleaseWriteIntent(*this, rp)) {
    return;
  }
  // The reservation is gone from the chain; keeping rp in the write set
  // would let a later write on the same slot skip AcquireWriteIntent and
  // reach RegisterVersionWrite with no pending intent.
  write_set_.erase(rp);
}

Transaction::VersionCacheShard& Transaction::ThreadShard() {
  // Readers must finish before the owner moves or destroys the transaction,
  // including its shard map, lock, and the references returned here.
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
  if (IsFinished()) {
    return StatusError(StatusCode::kRuntimeError,
                       "cannot read through a finished transaction");
  }
  if (transaction_manager_ != nullptr && transaction_manager_->FailStopped()) {
    return StatusError(StatusCode::kIOError,
                       "transaction manager is fail-stopped");
  }
  // Read-only scans can use the chain-free physical-image fast path. A
  // read-write transaction must consult the version shard even when the
  // page counter is zero: another worker using this transaction may have
  // published a chain entry just before incrementing that counter, and its
  // own staged value must remain visible across workers.
  if (transaction_manager_ != nullptr && physical && read_only_ &&
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
  std::optional<std::string> served = std::move(visible).MoveValue();
  if (!served) {
    // Unreachable: the nullopt branch above already returned a view of the
    // physical image, so a disengaged optional cannot get here.
    return Status::kNotExists;
  }
  VersionCacheShard& shard = ThreadShard();
  std::scoped_lock shard_lock(shard.mutex);
  constexpr size_t kMaxVersionReadCache = 4096;
  constexpr size_t kMaxRetiredViews = 4096;
  if (!read_only_) {
    // Read-write transactions never consult the cache (the lookup above is
    // read_only_-gated for the read-your-writes guarantee), so a keyed map
    // entry would be write-only overhead.  The retired pool is already the
    // bounded keep-alive for handed-out views; park the served string there
    // directly and bound it the same way.
    shard.retired.push_back(std::move(*served));
    if (shard.retired.size() > kMaxRetiredViews) {
      shard.retired.erase(shard.retired.begin(),
                          shard.retired.begin() + static_cast<std::ptrdiff_t>(
                                                      shard.retired.size() -
                                                      (kMaxRetiredViews / 2)));
    }
    return std::string_view(shard.retired.back());
  }
  const uint64_t epoch = write_epoch_.load(std::memory_order_acquire);
  // Retire the previously cached string for this key (if any): a caller may
  // still hold a view into it from an earlier read of the same row.
  auto existing = shard.entries.find(rp);
  if (existing != shard.entries.end()) {
    shard.retired.push_back(std::move(existing->second.value));
  }
  auto [iter, inserted] = shard.entries.insert_or_assign(
      rp, VersionCacheEntry{.epoch = epoch, .value = std::move(*served)});
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
  // this bounds the NUMBER of retired strings at roughly twice the
  // live-entry count. Each string is a full row image, so a workload of very
  // wide VARCHAR rows can still consume far more than 2x the live bytes;
  // the bound is on entry count, not memory.
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
  // Only read-only transactions populate their thread-local cache, and this
  // write path cannot be one; the epoch bump above invalidates every shard.
}

bool Transaction::CancelStagedVersion(const RowPosition& rp) {
  // Best-effort rollback helper called from Status-returning WAL error paths
  // (see RowPage::Insert/Update/Delete). The cleanup containers and callbacks
  // below (write_set_, ForgetVersionChain, the version shards) are not
  // noexcept, so an allocation failure in this path must not escape into the
  // Status boundary. Report an incomplete cancellation through the bool
  // result instead: callers either translate `false` into a Status (the
  // Update path) or have already marked the transaction failed.
  try {
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
      } else if (found->second.pending &&
                 found->second.pending->owner == ID() &&
                 found->second.pending->staged) {
        // Release the payload accounting before dropping the staged version:
        // every sibling path (ReleaseWriteIntent / AbortVersions /
        // CommitVersions) does this, and the write-set erase below means no
        // later abort can recover the charge if we skip it here.
        transaction_manager_->ReleaseVersionPayloadBytes(
            found->second.pending->payload.InlineBytes());
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
  } catch (...) {
    // Never let a cleanup failure escape the Status-returning WAL error path.
    return false;
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
  if (transaction_manager_ == nullptr ||
      transaction_manager_->logger_ == nullptr) {
    // Default-constructed ("for test purpose only") and moved-from
    // transactions have no WAL. Every other manager-dependent method degrades
    // safely; dereferencing logger_ here would segfault across a Status
    // boundary.
    return StatusError(StatusCode::kRuntimeError,
                       "transaction has no WAL logger");
  }
  if (transaction_manager_->FailStopped()) {
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
  const UnwindGuard clear_phase([&] { DebugSetPhase(nullptr); });
  LogRecord record = lr;
  record.prev_lsn = prev_lsn_.load(std::memory_order_relaxed);
  serialization_scratch_.clear();
  record.SerializeTo(serialization_scratch_);
  const size_t rec_bytes = serialization_scratch_.size();
  RETURN_IF_FAIL(transaction_manager_->logger_->CheckAppendSize(rec_bytes));
  // Reserve the serialized record before advancing the WAL. A transaction
  // without a recovery reader must keep every appended change for replay.
  const bool needs_staging =
      record.type != LogType::kBegin && record.type != LogType::kCommit &&
      record.type != LogType::kAbort &&
      record.type != LogType::kBeginCheckpoint &&
      record.type != LogType::kEndCheckpoint && !wal_reread_mode_;
  bool staged = false;
  if (needs_staging) {
    const bool can_reread = transaction_manager_->CanWalReread();
    const size_t limit =
        can_reread ? transaction_manager_->EffectiveWalRereadThresholdBytes()
                   : std::numeric_limits<size_t>::max();
    const bool within_limit =
        rec_bytes <= limit - std::min(staged_bytes_, limit);
    if (within_limit &&
        staged_charge_.TryAdd(rec_bytes, /*respect_soft_limit=*/false)) {
      try {
        staged_records_.push_back(std::move(record));
        staged = true;
      } catch (const std::bad_alloc&) {
        staged_charge_.Release(rec_bytes);
      } catch (const std::length_error&) {
        staged_charge_.Release(rec_bytes);
      }
    }
    if (!staged && !can_reread) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "cannot stage transaction WAL record");
    }
  }
  UnwindGuard undo_staging([&] {
    if (staged) {
      staged_records_.pop_back();
      staged_charge_.Release(rec_bytes);
    }
  });
  ASSIGN_OR_RETURN(
      lsn_t, start,
      transaction_manager_->logger_->AddLog(serialization_scratch_));
  prev_lsn_ = start;
  if (!has_log_.load(std::memory_order_relaxed)) {
    first_lsn_.store(start, std::memory_order_relaxed);
  }
  has_log_.store(true, std::memory_order_release);
  prev_end_lsn_ = start + static_cast<lsn_t>(rec_bytes);
  if (staged) {
    staged_records_.back().record_lsn = start;
    staged_bytes_ += rec_bytes;
  } else if (needs_staging) {
    wal_reread_mode_ = true;
    std::vector<LogRecord>().swap(staged_records_);
    staged_charge_.ReleaseAll();
    staged_bytes_ = 0;
    transaction_manager_->RecordWalRereadActivation();
  }
  undo_staging.Disarm();
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

Status Transaction::ReserveIndexUndoBytes(page_id_t pid, size_t bytes) {
  if (bytes == 0 || transaction_manager_ == nullptr) {
    return Status::kSuccess;
  }
  return transaction_manager_->ReserveIndexUndoBytes(*this, pid, bytes);
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

bool Transaction::NoWaitForConflicts() const {
  return transaction_manager_ != nullptr &&
         transaction_manager_->GetDeadlockPolicy() ==
             TransactionManager::DeadlockPolicy::kNoWait;
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

Status Transaction::CommitWait() const {
  if (transaction_manager_ == nullptr) {
    return Status::kSuccess;
  }
  return transaction_manager_->WaitForWalDurable(
      std::max(PrevRecordEndLSN(), DurabilityDependence()));
}

namespace {
std::atomic<int64_t>& StatementTimeoutOverrideMs() noexcept {
  static std::atomic<int64_t> val{-1};
  return val;
}
thread_local int64_t tl_statement_deadline_ns = 0;

Status CheckDeadlineNs(int64_t deadline) {
  if (deadline == 0) {
    return Status::kSuccess;
  }
  const int64_t now_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  if (now_ns >= deadline) {
    return StatusError(StatusCode::kQueryCanceled,
                       "statement timeout exceeded");
  }
  return Status::kSuccess;
}
}  // namespace

void Transaction::SetDefaultStatementTimeoutForTest(
    std::chrono::milliseconds timeout) noexcept {
  StatementTimeoutOverrideMs().store(timeout.count(),
                                     std::memory_order_release);
}

std::chrono::milliseconds Transaction::DefaultStatementTimeout() noexcept {
  const int64_t over =
      StatementTimeoutOverrideMs().load(std::memory_order_acquire);
  if (over >= 0) {
    return std::chrono::milliseconds(over);
  }
  static const int64_t kEnvTimeoutMs = []() -> int64_t {
    const char* raw =
        ::tinylamb::platform::GetEnv("TINYLAMB_STATEMENT_TIMEOUT_MS");
    if (raw == nullptr || *raw == '\0') {
      return 0;
    }
    char* end = nullptr;
    if (raw[0] == '-') {
      return 0;
    }
    errno = 0;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno == ERANGE || end == raw || *end != '\0' ||
        parsed > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return 0;
    }
    return static_cast<int64_t>(parsed);
  }();
  return std::chrono::milliseconds(kEnvTimeoutMs);
}

namespace {
// ms * 1e6 and now_ns + delta can both overflow int64 for extreme timeout
// values (e.g. a corrupt TINYLAMB_STATEMENT_TIMEOUT_MS). Saturate instead of
// invoking undefined behavior, which would make the deadline wrap into the
// past (instant timeout) or a small positive value (effectively unbounded).
int64_t SaturatingDeadlineNs(int64_t now_ns, int64_t ms) noexcept {
  if (ms <= 0) {
    return 0;
  }
  int64_t delta = 0;
  if (__builtin_mul_overflow(ms, INT64_C(1000000), &delta) || delta < 0) {
    return INT64_MAX;
  }
  int64_t deadline = 0;
  if (__builtin_add_overflow(now_ns, delta, &deadline)) {
    return INT64_MAX;
  }
  return deadline;
}
}  // namespace

void Transaction::SetStatementTimeout(
    std::chrono::milliseconds timeout) noexcept {
  const int64_t ms = timeout.count();
  statement_timeout_ms_.store(ms, std::memory_order_release);
  if (ms > 0) {
    const int64_t now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    statement_deadline_ns_.store(SaturatingDeadlineNs(now_ns, ms),
                                 std::memory_order_release);
  } else {
    statement_deadline_ns_.store(0, std::memory_order_release);
  }
}

std::chrono::milliseconds Transaction::StatementTimeout() const noexcept {
  const int64_t ms = statement_timeout_ms_.load(std::memory_order_acquire);
  if (ms >= 0) {
    return std::chrono::milliseconds(ms);
  }
  return DefaultStatementTimeout();
}

void Transaction::BeginStatementTimer() noexcept {
  const int64_t ms = StatementTimeout().count();
  if (ms > 0) {
    const int64_t now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    statement_deadline_ns_.store(SaturatingDeadlineNs(now_ns, ms),
                                 std::memory_order_release);
  } else {
    statement_deadline_ns_.store(0, std::memory_order_release);
  }
}

Status Transaction::CheckStatementTimeout() const noexcept {
  return CheckDeadlineNs(
      statement_deadline_ns_.load(std::memory_order_relaxed));
}

Transaction::ScopedThreadStatementDeadline::ScopedThreadStatementDeadline(
    int64_t deadline_ns) noexcept
    : prev_deadline_ns_(tl_statement_deadline_ns) {
  if (deadline_ns != 0) {
    tl_statement_deadline_ns = prev_deadline_ns_ == 0
                                   ? deadline_ns
                                   : std::min(prev_deadline_ns_, deadline_ns);
  }
}

Transaction::ScopedThreadStatementDeadline::~ScopedThreadStatementDeadline() {
  tl_statement_deadline_ns = prev_deadline_ns_;
}

Status Transaction::CheckThreadStatementTimeout() noexcept {
  return CheckDeadlineNs(tl_statement_deadline_ns);
}

}  // namespace tinylamb
