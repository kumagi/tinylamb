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

#ifndef TINYLAMB_TRANSACTION_HPP
#define TINYLAMB_TRANSACTION_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "page/page.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class TransactionManager;
class CheckpointManager;
class PageManager;
class Logger;
class LockManager;
struct FosterPair;
struct LogRecord;
struct Row;

class TransactionWriteSet final {
 public:
  using value_type = RowPosition;
  using size_type = std::size_t;

 private:
  template <typename VectorIterator, typename SetIterator>
  class BasicIterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = RowPosition;
    using difference_type = std::ptrdiff_t;
    using pointer = const RowPosition*;
    using reference = const RowPosition&;

    BasicIterator() = default;
    BasicIterator(VectorIterator vector_iterator)
        : value_(std::in_place_index<0>, vector_iterator) {}
    BasicIterator(SetIterator set_iterator)
        : value_(std::in_place_index<1>, set_iterator) {}

    reference operator*() const {
      if (std::holds_alternative<VectorIterator>(value_)) {
        return *std::get<VectorIterator>(value_);
      }
      return *std::get<SetIterator>(value_);
    }

    pointer operator->() const { return std::addressof(operator*()); }

    BasicIterator& operator++() {
      if (std::holds_alternative<VectorIterator>(value_)) {
        ++std::get<VectorIterator>(value_);
      } else {
        ++std::get<SetIterator>(value_);
      }
      return *this;
    }

    BasicIterator operator++(int) {
      BasicIterator previous = *this;
      ++*this;
      return previous;
    }

    friend bool operator==(const BasicIterator& lhs, const BasicIterator& rhs) {
      return lhs.value_ == rhs.value_;
    }

    friend bool operator!=(const BasicIterator& lhs, const BasicIterator& rhs) {
      return !(lhs == rhs);
    }

   private:
    friend class TransactionWriteSet;
    std::variant<VectorIterator, SetIterator> value_;
  };

 public:
  using iterator = BasicIterator<std::vector<RowPosition>::iterator,
                                 std::unordered_set<RowPosition>::iterator>;
  using const_iterator =
      BasicIterator<std::vector<RowPosition>::const_iterator,
                    std::unordered_set<RowPosition>::const_iterator>;

  iterator begin() {
    return spilled_ ? iterator(spill_.begin()) : iterator(small_.begin());
  }

  iterator end() {
    return spilled_ ? iterator(spill_.end()) : iterator(small_.end());
  }

  const_iterator begin() const {
    return spilled_ ? const_iterator(spill_.begin())
                    : const_iterator(small_.begin());
  }

  const_iterator end() const {
    return spilled_ ? const_iterator(spill_.end())
                    : const_iterator(small_.end());
  }

  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  [[nodiscard]] bool contains(const value_type& value) const {
    if (spilled_) {
      return spill_.contains(value);
    }
    const auto position =
        std::lower_bound(small_.begin(), small_.end(), value, Less{});
    return position != small_.end() && *position == value;
  }

  std::pair<iterator, bool> insert(const value_type& value) {
    if (spilled_) {
      auto [position, inserted] = spill_.insert(value);
      return {iterator(position), inserted};
    }

    const auto position =
        std::lower_bound(small_.begin(), small_.end(), value, Less{});
    if (position != small_.end() && *position == value) {
      return {iterator(position), false};
    }

    if (small_.size() < kSmallCapacity) {
      const auto offset = position - small_.begin();
      small_.insert(position, value);
      return {iterator(small_.begin() + offset), true};
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
    return {iterator(spill_.find(value)), true};
  }

  size_type erase(const value_type& value) {
    if (spilled_) {
      return spill_.erase(value);
    }
    const auto position =
        std::lower_bound(small_.begin(), small_.end(), value, Less{});
    if (position == small_.end() || *position != value) {
      return 0;
    }
    small_.erase(position);
    return 1;
  }

  iterator erase(iterator position) {
    if (spilled_) {
      if (std::holds_alternative<std::unordered_set<RowPosition>::iterator>(
              position.value_)) {
        return {
            spill_.erase(std::get<std::unordered_set<RowPosition>::iterator>(
                position.value_))};
      }
      return end();
    }
    if (std::holds_alternative<std::vector<RowPosition>::iterator>(
            position.value_)) {
      auto vector_position =
          std::get<std::vector<RowPosition>::iterator>(position.value_);
      const auto offset = vector_position - small_.begin();
      small_.erase(vector_position);
      return {small_.begin() + offset};
    }
    return end();
  }

  template <typename Predicate>
  size_type erase_if(Predicate predicate) {
    size_type removed = 0;
    for (auto position = begin(); position != end();) {
      if (predicate(*position)) {
        position = erase(position);
        ++removed;
      } else {
        ++position;
      }
    }
    return removed;
  }

  void clear() {
    std::vector<RowPosition>().swap(small_);
    std::unordered_set<RowPosition>().swap(spill_);
    spilled_ = false;
  }

  [[nodiscard]] bool empty() const { return small_.empty() && spill_.empty(); }
  [[nodiscard]] size_type size() const { return small_.size() + spill_.size(); }

 private:
  static constexpr size_type kSmallCapacity = 64;

  struct Less {
    bool operator()(const RowPosition& lhs, const RowPosition& rhs) const {
      if (lhs.page_id != rhs.page_id) {
        return lhs.page_id < rhs.page_id;
      }
      return lhs.slot < rhs.slot;
    }
  };

  std::vector<RowPosition> small_{};
  std::unordered_set<RowPosition> spill_{};
  bool spilled_{false};
};

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted,
};

class TransactionFinishHook {
 public:
  TransactionFinishHook() = default;
  virtual ~TransactionFinishHook() = default;
  TransactionFinishHook(const TransactionFinishHook&) = delete;
  TransactionFinishHook& operator=(const TransactionFinishHook&) = delete;
  TransactionFinishHook(TransactionFinishHook&&) = delete;
  TransactionFinishHook& operator=(TransactionFinishHook&&) = delete;
  virtual void OnTransactionFinished(TransactionStatus status) noexcept = 0;
};

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t);

class Transaction final {
 public:
  Transaction() = default;  // For test purpose only.
  Transaction(txn_id_t txn_id, TransactionManager* tm, bool read_only = false);
  Transaction(const Transaction& o) = delete;
  ~Transaction() noexcept;
  Transaction(Transaction&& o) noexcept;
  Transaction& operator=(const Transaction& o) = delete;
  Transaction& operator=(Transaction&& o) noexcept {
    if (this == &o) {
      return *this;
    }
    FinishRunningNoexcept();
    txn_id_ = o.txn_id_;
    snapshot_ts_ = o.snapshot_ts_;
    read_set_ = std::move(o.read_set_);
    write_set_ = std::move(o.write_set_);
    recycled_page_allocations_ = std::move(o.recycled_page_allocations_);
    mutated_index_roots_ = std::move(o.mutated_index_roots_);
    index_undo_reservations_ = std::move(o.index_undo_reservations_);
    shard_epoch_ = o.shard_epoch_;
    write_epoch_.store(o.write_epoch_.load(std::memory_order_acquire),
                       std::memory_order_release);
    version_read_caches_ = std::move(o.version_read_caches_);
    read_state_mutex_ = std::move(o.read_state_mutex_);
    serialization_scratch_ = std::move(o.serialization_scratch_);
    finish_hooks_ = std::move(o.finish_hooks_);
    finish_hooks_finalized_ = o.finish_hooks_finalized_;
    failed_.store(o.failed_.load(std::memory_order_acquire),
                  std::memory_order_release);
    // chain_mu_ is deliberately NOT transferred: every Transaction object
    // owns a fresh mutex. Transferring (nulling the moved-from object's)
    // opened a window where the checkpoint's active-transaction snapshot --
    // running between Begin()'s registry unlock and the move constructor's
    // registry repoint -- dereferenced a moved-from null mutex.
    prev_lsn_.store(o.prev_lsn_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    first_lsn_.store(o.first_lsn_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
    has_log_.store(o.has_log_.load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
    has_table_drop_.store(o.has_table_drop_.load(std::memory_order_acquire),
                          std::memory_order_release);
    prev_end_lsn_.store(o.prev_end_lsn_.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    status_.store(o.status_.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
    transaction_manager_ = o.transaction_manager_;
    read_only_ = o.read_only_;
    durability_dependence_.store(
        o.durability_dependence_.load(std::memory_order_acquire),
        std::memory_order_release);
    // Carry the victim flag: a Wound() issued right before the move would
    // otherwise be forgotten and the victim would keep running (and keep
    // its write intents) forever.
    wounded_.store(o.wounded_.load(std::memory_order_acquire),
                   std::memory_order_release);
    abort_started_.store(o.abort_started_.load(std::memory_order_acquire),
                         std::memory_order_release);
    intent_wait_failure_ = o.intent_wait_failure_;
    if (o.transaction_manager_ != nullptr) {
      o.transaction_manager_->MoveActiveTransaction(&o, this);
    }
    // Neutralize the moved-from object (PageRef-style): it no longer owns a
    // registry slot or manager services.
    o.transaction_manager_ = nullptr;
    o.finish_hooks_.clear();
    o.finish_hooks_finalized_ = true;
    return *this;
  }

  void SetStatus(TransactionStatus status) {
    // Relaxed: the checkpoint thread reads status_ and prev_lsn_ for its
    // ActiveTransactionTable snapshot while holding transaction_table_lock;
    // the relaxed atomics keep the field reads themselves race-free.
    // Consumers only need a self-consistent per-field value, not cross-field
    // ordering.
    status_.store(status, std::memory_order_relaxed);
  }
  void MarkTableDrop() {
    has_table_drop_.store(true, std::memory_order_release);
  }
  void AddFinishHook(std::shared_ptr<TransactionFinishHook> hook) {
    if (hook != nullptr) {
      finish_hooks_.push_back(std::move(hook));
    }
  }
  void FinalizeFinishHooks() noexcept;
  bool IsFinished() const {
    const TransactionStatus current = status_.load(std::memory_order_relaxed);
    return current == TransactionStatus::kCommitted ||
           current == TransactionStatus::kAborted;
  }
  lsn_t PrevLSN() const { return prev_lsn_.load(std::memory_order_relaxed); }
  // Start LSN of this transaction's FIRST WAL record. The checkpoint's
  // recycle floor takes the minimum over active transactions so a segment
  // an undo chain may still walk into is never recycled; zero also denotes
  // no record, so use HasLogRecord() to distinguish that case.
  lsn_t FirstLSN() const { return first_lsn_.load(std::memory_order_relaxed); }
  [[nodiscard]] bool HasLogRecord() const {
    return has_log_.load(std::memory_order_acquire);
  }
  // End LSN (= start + record bytes) of this transaction's most recently
  // appended WAL record.  Page stamps must use this, not PrevLSN(): the
  // durability gate waits for DurableLSN() >= page_lsn, and waiting on the
  // record's START only proves the bytes BEFORE it are durable, so a page
  // could reach disk while its own modifying record is still volatile
  // (WAL-before-data violation, docs/recovery_invariants.md).
  lsn_t PrevRecordEndLSN() const {
    return prev_end_lsn_.load(std::memory_order_relaxed);
  }

  // D4 (docs/design.md): WAL durability and external visibility are separate.
  // A reader that observes another transaction's committed MVCC version
  // records that commit's durable LSN here (running maximum).  Before any
  // result derived from it reaches the user -- including for a read-only
  // transaction -- the commit path waits for this LSN to be durable.  A zero
  // value means the transaction observed nothing that needs a barrier.
  void RecordDurabilityDependence(lsn_t durable_lsn) const {
    lsn_t cur = durability_dependence_.load(std::memory_order_relaxed);
    while (cur < durable_lsn &&
           !durability_dependence_.compare_exchange_weak(
               cur, durable_lsn, std::memory_order_relaxed)) {
    }
  }
  [[nodiscard]] lsn_t DurabilityDependence() const {
    return durability_dependence_.load(std::memory_order_acquire);
  }

  bool AddReadSet(const RowPosition& rp);
  bool AddWriteSet(const RowPosition& rp);
  [[nodiscard]] bool HasWriteSet(const RowPosition& rp) const {
    return write_set_.contains(rp);
  }
  // Like AddWriteSet, but installs `before` as the chain's base committed
  // version so concurrent readers keep seeing the old row until the write is
  // staged (prevents a phantom kNotExists during the intent window).
  bool AddWriteSet(const RowPosition& rp, std::string_view before);
  bool TryAddWriteSet(const RowPosition& rp);
  // True when the position's committed chain still serves a live row under
  // this transaction's snapshot.  RowPage::Insert consults this before
  // reusing a deleted slot: an insert staged at a position whose old row is
  // still visible to the inserter's own snapshot would mask that row behind
  // the pending staged value (ReadVersion serves pending first), silently
  // dropping it from every scan the snapshot is entitled to.
  [[nodiscard]] bool SnapshotSeesRow(const RowPosition& rp) const;
  // Drops a previously acquired, still-unstaged write intent (used when
  // Insert picked a hole that SnapshotSeesRow disqualifies).  Staged or
  // foreign intents are left untouched.
  void ReleaseWriteIntent(const RowPosition& rp);
  // resolve_head=false is a pure snapshot read: an unstaged write intent
  // this transaction happens to hold on the position does NOT upgrade the
  // result to the newest committed version.  Write-path resolution keeps
  // the default (head under an own intent).
  StatusOr<std::string_view> ReadVersion(
      const RowPosition& rp, std::optional<std::string_view> physical,
      bool resolve_head = true);
  void RegisterVersionWrite(const RowPosition& rp,
                            std::optional<std::string_view> before,
                            std::optional<std::string_view> after);
  bool CancelStagedVersion(const RowPosition& rp);
  void MarkFailed() noexcept { failed_.store(true, std::memory_order_release); }
  [[nodiscard]] bool IsFailed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }
  void InvalidateVersionReadCache() {
    write_epoch_.fetch_add(1, std::memory_order_release);
  }

  [[nodiscard]] txn_id_t ID() const { return txn_id_; }
  [[nodiscard]] uint64_t SnapshotTimestamp() const { return snapshot_ts_; }
  [[nodiscard]] bool RequiresHistoricalRead() const;
  [[nodiscard]] bool IndexKeysMayBeStale() const;
  [[nodiscard]] bool IndexKeysMayBeStale(page_id_t index_root) const;
  [[nodiscard]] TransactionManager::IndexReadGuard AcquireIndexReadGuard(
      page_id_t index_root) const {
    return transaction_manager_->AcquireIndexReadGuard(index_root);
  }
  [[nodiscard]] std::unique_lock<std::shared_mutex> AcquireIndexWriteGuard(
      page_id_t index_root) const {
    return transaction_manager_->AcquireIndexWriteGuard(index_root);
  }
  [[nodiscard]] std::vector<std::unique_lock<std::shared_mutex>>
  AcquireIndexWriteGuards(const std::vector<page_id_t>& index_roots) const {
    return transaction_manager_->AcquireIndexWriteGuards(index_roots);
  }
  [[nodiscard]] bool HasPageVersionChains(page_id_t page_id) const {
    return transaction_manager_ != nullptr &&
           transaction_manager_->HasPageVersionChains(page_id);
  }
  // Bytes on |pid| that OTHER transactions' unresolved undo may still claim
  // (see TransactionManager::ReserveIndexUndoBytes). Admission checks
  // subtract it from free_size_; callers without a manager (recovery redo)
  // read zero. Own reservations never block own operations: the reverse-chain
  // undo frees a later op's bytes before an earlier op's undo re-claims them.
  // Diagnostic: render the outstanding reservation owners on |pid| (test
  // support for the reservation regressions; not on any hot path).
  [[nodiscard]] std::string DebugDumpIndexReservations(page_id_t pid) const {
    return transaction_manager_ == nullptr
               ? ""
               : transaction_manager_->DebugDumpIndexReservations(pid);
  }
  [[nodiscard]] size_t IndexUndoReservedBytes(page_id_t pid) const {
    return transaction_manager_ == nullptr
               ? 0
               : transaction_manager_->IndexUndoReservedBytes(pid, txn_id_);
  }
  // Latch-free wait for the OTHER transactions' reservations on |pid| to
  // resolve (bounded by the write-intent wait limit; kConflicts at the
  // deadline). Callers must have released the page latch first; see
  // TransactionManager::WaitForIndexReservationDrain.
  Status WaitIndexReservationsDrain(page_id_t pid) const {
    if (transaction_manager_ == nullptr) {
      return Status::kSuccess;
    }
    return transaction_manager_->WaitForIndexReservationDrain(
        pid, txn_id_, transaction_manager_->GetWriteIntentWaitLimit());
  }
  [[nodiscard]] bool HasForeignIndexReservations() const {
    return transaction_manager_ != nullptr &&
           transaction_manager_->HasForeignIndexReservations(txn_id_);
  }
  // Coarse drain: wait until no OTHER transaction holds ANY index-undo
  // reservation (bounded by the write-intent wait limit; kConflicts at the
  // deadline). Latch-free by contract.
  Status WaitAllIndexReservationsDrain() const {
    return WaitAllIndexReservationsDrain(
        transaction_manager_ == nullptr
            ? std::chrono::milliseconds(0)
            : transaction_manager_->GetWriteIntentWaitLimit());
  }
  // Explicit-budget variant for callers that hold a lock the drain must not
  // ride (see BPlusTree: an index-gate holder waiting out a multi-second
  // reservation manufactures a gate x reservation cycle the detector cannot
  // see). A zero limit with a null manager still reports success.
  Status WaitAllIndexReservationsDrain(std::chrono::milliseconds limit) const {
    if (transaction_manager_ == nullptr) {
      return Status::kSuccess;
    }
    return transaction_manager_->WaitForAllIndexReservationsDrain(txn_id_,
                                                                  limit, this);
  }
  // B+Tree's in-gate drain points (FindLeaf / LeafInsert) set this when their
  // short budget expires: the kNoSpace they return then means "a foreign
  // index-undo reservation outlived the budget", not "the page is genuinely
  // full". Table::IndexInsert/IndexDelete consume it to re-run the guarded
  // RMW with the write gate released -- the release is what lets the
  // reservation owners finish and is why the guarded body cannot just wait
  // longer. Statement-scoped: a Transaction move does not carry it.
  void NoteIndexReservationDrainExhausted() const noexcept {
    index_drain_exhausted_.fetch_add(1, std::memory_order_relaxed);
  }
  bool ConsumeIndexReservationDrainExhausted() const noexcept {
    return index_drain_exhausted_.exchange(0, std::memory_order_relaxed) != 0;
  }
  // Index insertions need no stale-key fallback: MVCC hides rows newer than
  // the snapshot. Deletion/replacement can remove a key an old snapshot still
  // needs, so Table records those roots here and commit publishes their epoch.
  void MarkIndexKeysChanged(page_id_t index_root) {
    // Transactions touch a handful of index roots at most; a linear scan
    // avoids building a hashtable per transaction (a measurable share of the
    // TPC-C malloc/rehash profile).
    if (std::find(mutated_index_roots_.begin(), mutated_index_roots_.end(),
                  index_root) != mutated_index_roots_.end()) {
      return;
    }
    mutated_index_roots_.push_back(index_root);
    if (transaction_manager_ != nullptr) {
      transaction_manager_->MarkPendingIndexMutation(index_root);
    }
  }
  [[nodiscard]] bool IsReadOnly() const { return read_only_; }

  // Marks this transaction as wounded (Wound-Wait victim or a deadlock-
  // detector victim). AcquireWriteIntent checks this on every entry; the
  // next call returns false and the caller aborts the transaction, releasing
  // the held intent cleanly. Idempotent.
  void Wound() noexcept { wounded_.store(true, std::memory_order_release); }
  [[nodiscard]] bool IsWounded() const noexcept {
    return wounded_.load(std::memory_order_acquire);
  }
  // Set when TransactionManager::Abort begins unwinding this transaction.
  // A waiter blocked on this transaction's write intent checks the flag:
  // an aborting holder will roll its write back, so waiting for the intent
  // is pointless -- and the holder's undo walk may need the very page latch
  // the waiter is holding (RowPage ops run exclusively latched), which makes
  // the wait a latch cycle only the WAITER can break. Bounded by no timeout:
  // the flag is definitive, unlike the (last-resort) wait deadline.
  void MarkAbortStarted() noexcept {
    abort_started_.store(true, std::memory_order_release);
  }
  [[nodiscard]] bool IsAbortStarted() const noexcept {
    return abort_started_.load(std::memory_order_acquire);
  }
  // Diagnostic (round 9): where this transaction is currently parked, set
  // just before each potentially-blocking engine wait (write-intent wait,
  // tail reservation wait, durability barrier, undo walk, WAL append) and
  // cleared after. Read by RecordIntentWaitTimeout's holder dump so a
  // timeout postmortem names WHERE the blocking holder is stuck, not just
  // that it is. Best-effort: never load-bearing for correctness.
  void DebugSetPhase(const char* phase) const noexcept {
    debug_phase_.store(phase, std::memory_order_relaxed);
  }
  // Diagnostic (round 9): OS thread that runs this transaction, so a stall
  // postmortem can sample /proc/<pid>/task/<tid>/wchan for the holder.
  void DebugSetTid(int64_t tid) noexcept {
    debug_tid_.store(tid, std::memory_order_relaxed);
  }
  // Why the last AcquireWriteIntent (via AddWriteSet) returned false.  Owner-
  // thread confined: written by the thread that runs the transaction's writes
  // and read by that same thread's executor to build the conflict diagnostic.
  enum class IntentWaitFailure : uint8_t {
    kNone = 0,
    kConflict = 1,  // policy rejected the wait (wait-die) or try-lock lost
    kWounded = 2,   // this transaction is a deadlock/wound victim
    kHolderAborting = 3,  // the intent holder has started rolling back
    kTimeout = 4,  // the write-intent wait limit fired (latch-cycle hatch)
    kFailStopped = 5,
  };
  void RecordIntentWaitFailure(IntentWaitFailure reason) {
    intent_wait_failure_ = reason;
  }
  [[nodiscard]] IntentWaitFailure LastIntentWaitFailure() const {
    return intent_wait_failure_;
  }

  Status PreCommit();
  Status Abort();

  // Appends `lr` to the WAL, links it into this transaction's prev_lsn
  // chain, and records the record's END LSN for page stamping.  Returns the
  // record's start LSN (the chain pointer).
  StatusOr<lsn_t> AppendLog(const LogRecord& lr);

  // Log the action. Returns LSN.
  StatusOr<lsn_t> InsertLog(page_id_t pid, slot_t slot, std::string_view redo);
  StatusOr<lsn_t> InsertLeafLog(page_id_t pid, std::string_view key,
                                std::string_view redo);
  StatusOr<lsn_t> InsertBranchLog(page_id_t pid, std::string_view key,
                                  page_id_t redo);

  StatusOr<lsn_t> UpdateLog(page_id_t pid, slot_t slot, std::string_view redo,
                            std::string_view undo);
  StatusOr<lsn_t> AppendPhysicalUpdateCompensation(page_id_t pid, slot_t slot,
                                                   std::string_view redo);
  StatusOr<lsn_t> UpdateLeafLog(page_id_t pid, std::string_view key,
                                std::string_view redo, std::string_view undo);
  StatusOr<lsn_t> UpdateBranchLog(page_id_t pid, std::string_view key,
                                  page_id_t redo, page_id_t undo);

  StatusOr<lsn_t> DeleteLog(page_id_t pid, slot_t slot, std::string_view undo);
  StatusOr<lsn_t> DeleteLeafLog(page_id_t pid, std::string_view key,
                                std::string_view undo);
  StatusOr<lsn_t> DeleteBranchLog(page_id_t pid, std::string_view key,
                                  page_id_t undo);

  StatusOr<lsn_t> SetLowestLog(page_id_t pid, page_id_t redo, page_id_t undo);
  StatusOr<lsn_t> SetRowPageNextLog(page_id_t pid, page_id_t redo,
                                    page_id_t undo);
  StatusOr<lsn_t> SetRowPagePrevLog(page_id_t pid, page_id_t redo,
                                    page_id_t undo);

  StatusOr<lsn_t> SetLowFence(page_id_t pid, const IndexKey& redo,
                              const IndexKey& undo);
  StatusOr<lsn_t> SetHighFence(page_id_t pid, const IndexKey& redo,
                               const IndexKey& undo);
  StatusOr<lsn_t> SetFoster(page_id_t pid, const FosterPair& redo,
                            const FosterPair& undo);

  // Registers the bytes this transaction's UNDO will re-consume on |pid| once
  // a logged mutation frees them (delete re-insert, shrink update, fence or
  // foster restore). Called by the log emitters right after the record's
  // append succeeded; the reservation is released when the transaction leaves
  // the active table. See TransactionManager::ReserveIndexUndoBytes.
  void ReserveIndexUndoBytes(page_id_t pid, size_t bytes);

  StatusOr<lsn_t> AllocatePageLog(page_id_t page_id, PageType new_page_type,
                                  PageAllocationKind allocation_kind,
                                  PageType old_page_type,
                                  std::string old_target_payload,
                                  std::string old_meta_payload);
  StatusOr<lsn_t> AllocatePageLog(page_id_t page_id, PageType new_page_type);

  // D3 (docs/design.md): destroy records carry the old page type and, when
  // the page held rows, a full body image so undo restores an aborted
  // destroy exactly.
  StatusOr<lsn_t> DestroyPageLog(page_id_t page_id,
                                 PageType old_page_type = PageType::kUnknown,
                                 std::string old_page_body = {});

  // Prepared mainly for testing.
  // Using this function is discouraged to get performance of flush pipelining.
  // Note this waits only until the transaction's records are WRITTEN to the
  // WAL (CommittedLSN), not fsynced — it is a visibility barrier, not a
  // durability barrier; use Logger::WaitForDurable for the latter.
  void CommitWait() const;

  PageManager* GetPageManager() {
    return transaction_manager_->GetPageManager();
  }

  // Drops MVCC version chains left by a previous incarnation of |pid|.
  // Invoked by PageManager::AllocateNewPage when it recycles a page id off
  // the free list; see TransactionManager::InvalidatePageVersions.
  void InvalidatePageVersions(page_id_t pid) {
    if (transaction_manager_ != nullptr) {
      transaction_manager_->InvalidatePageVersions(pid, this);
    }
  }

  void RecordRecycledPageAllocation(page_id_t pid) {
    recycled_page_allocations_.push_back(pid);
  }

  // Diagnostic dump of every version chain on |pid|; empty string when no
  // transaction manager is attached.  Session-fuzzer mismatch reports only.
  [[nodiscard]] std::string DebugDumpVersionChains(page_id_t pid) const {
    return transaction_manager_ != nullptr
               ? transaction_manager_->DebugDumpVersionChains(pid)
               : "";
  }

  // Whether |id| still names a live transaction.  Used by the page
  // allocator to keep a page id whose destroy is still undoable out of the
  // allocation path.
  [[nodiscard]] bool IsTransactionActive(txn_id_t id) const {
    return transaction_manager_ != nullptr &&
           transaction_manager_->IsActive(id);
  }
  friend std::ostream& operator<<(std::ostream& o, const Transaction& t) {
    o << "Transaction(id=" << t.txn_id_ << ", status=" << t.status_
      << ", prev_lsn=" << t.prev_lsn_ << ", read_set=" << t.read_set_.size()
      << ", write_set=" << t.write_set_.size() << ")";
    return o;
  }

  // Transaction is not a value object. Never try to compare by its attributes.
  bool operator==(const Transaction& rhs) const = delete;

 private:
  friend class TransactionManager;
  friend class CheckpointManager;

  void FinishRunningNoexcept() noexcept;

  // Version cache sharded per reading thread, each with its own lock.  Scan
  // workers read concurrently through one transaction: steady-state reads
  // touch only their own (uncontended) shard, and a shard eviction can only
  // invalidate string views previously handed to the SAME thread, which by
  // construction consumed them before its next read.  The shard map itself is
  // guarded by read_state_mutex_; per-call access goes through a thread-local
  // shortcut so it is taken once per thread, not once per row.
  //
  // Every entry carries the transaction's write epoch at insertion time.
  // RegisterVersionWrite bumps the epoch, so entries in all shards go stale
  // at once without cross-thread erasure (which would dangle views other
  // workers still hold).  A cached entry may be served only while
  // CacheEntryCurrent(entry.epoch) holds -- see ReadVersion for where this
  // is enforced; it is what keeps any worker under this transaction from
  // reading a pre-write value out of its shard's cache.
  struct VersionCacheEntry {
    uint64_t epoch{0};
    std::string value;
  };
  struct VersionCacheShard {
    std::mutex mutex;
    std::unordered_map<RowPosition, VersionCacheEntry> entries;
    // Strings evicted from `entries` while a caller may still hold a
    // string_view into them.  ReadVersion hands out views into this shard's
    // strings; recycling a node's storage immediately would leave those
    // views dangling after kMaxVersionReadCache subsequent reads of other
    // rows.  Retired strings are freed in batches once the pool itself is
    // refilled, which bounds memory at ~2x the live cache while making the
    // safe-consumption window thousands of times wider than the eviction
    // boundary.
    std::vector<std::string> retired;
  };
  // Returns this calling thread's version cache shard in this transaction.
  VersionCacheShard& ThreadShard();
  [[nodiscard]] static uint64_t NextShardEpoch();
  [[nodiscard]] bool CacheEntryCurrent(uint64_t entry_epoch) const {
    return entry_epoch == write_epoch_.load(std::memory_order_acquire);
  }

 public:
  // Current write generation; bumped by every RegisterVersionWrite.  Cache
  // entries tagged with any older generation are logically invalidated.
  [[nodiscard]] uint64_t WriteEpoch() const {
    return write_epoch_.load(std::memory_order_acquire);
  }

 private:
  txn_id_t txn_id_{static_cast<txn_id_t>(-1)};
  uint64_t snapshot_ts_{0};

  std::unordered_set<RowPosition> read_set_{};
  // Owner-thread confined: writes are issued from the transaction's owning
  // thread only (reads may register concurrently from join workers, hence
  // read_state_mutex_; writes also drive AcquireWriteIntent whose shard
  // locks must never nest under a transaction-local mutex).
  TransactionWriteSet write_set_{};
  std::vector<page_id_t> recycled_page_allocations_{};
  // Index roots whose delete/update keys this transaction removed. A vector
  // with linear dedup in MarkIndexKeysChanged: the per-transaction count is
  // tiny, and an unordered_set here showed up as hashtable rehash churn on
  // every transaction in the TPC-C profile.
  std::vector<page_id_t> mutated_index_roots_{};
  // Accumulated (page, bytes) pairs this transaction's undo may still claim;
  // linear dedup like mutated_index_roots_ -- a transaction touches a handful
  // of index pages at most. Cleared by the manager when the reservation is
  // released (ForgetTransaction), so a repeated finish cannot double-release.
  std::vector<std::pair<page_id_t, uint64_t>> index_undo_reservations_{};
  // Process-wide generation, unique across every Transaction ever created.
  // Thread-local shard caches key on it so a destroyed transaction whose
  // address (or even id, across Database instances) is reused can never be
  // mistaken for the cached owner.
  uint64_t shard_epoch_{0};
  // Write generation for version cache invalidation (see VersionCacheEntry).
  std::atomic<uint64_t> write_epoch_{0};
  std::unordered_map<std::thread::id, std::unique_ptr<VersionCacheShard>>
      version_read_caches_{};
  // A read-only query may hand page morsels to multiple scan workers.  The
  // version cache and read set are transaction-local, so guard them while
  // preserving a single MVCC snapshot across those workers.
  std::unique_ptr<std::mutex> read_state_mutex_{std::make_unique<std::mutex>()};
  // Serializes WAL chain maintenance ({AppendLog, PreCommit's commit record,
  // Abort's terminator} + the prev_lsn_/status stores) against the
  // checkpoint's active-transaction snapshot, which takes this mutex per
  // transaction while reading status_/PrevLSN/FirstLSN.  Per-transaction by
  // design: a manager-wide lock there serialized every WAL append of every
  // transaction on one contended mutex.  Owned per OBJECT and never moved:
  // the registry may briefly reference a moved-from Transaction (between
  // Begin()'s registry unlock and the move's re-registration), so this
  // mutex must stay valid and lockable on the moved-from object too.
  mutable std::mutex chain_mu_{};
  std::string serialization_scratch_{};
  // Relaxed atomics: writers (the owning thread, via AddLog/SetStatus) do
  // not hold transaction_table_lock, but the checkpoint thread reads both
  // fields for its ActiveTransactionTable snapshot while holding it.  The
  // atomics remove the data race; no cross-field ordering is required.
  std::atomic<lsn_t> prev_lsn_{0};
  // End LSN of the record that starts at prev_lsn_ (see PrevRecordEndLSN).
  std::atomic<lsn_t> prev_end_lsn_{0};
  // Start LSN of the first record this transaction ever appended (see
  // FirstLSN). Compare-exchanged so a concurrent reader always sees the
  // earliest value.
  std::atomic<lsn_t> first_lsn_{0};
  std::atomic<bool> has_log_{false};
  std::atomic<bool> has_table_drop_{false};
  std::vector<std::shared_ptr<TransactionFinishHook>> finish_hooks_;
  bool finish_hooks_finalized_{false};
  // D4: highest dependency commit LSN observed by this transaction (see
  // RecordDurabilityDependence).  Updated from const read paths.
  mutable std::atomic<lsn_t> durability_dependence_{0};
  std::atomic<TransactionStatus> status_{TransactionStatus::kUnknown};
  std::atomic<bool> failed_{false};
  bool read_only_{false};

  // Not owned by this class.
  TransactionManager* transaction_manager_{nullptr};
  // Set by an older transaction's Wound-Wait preemption or by a deadlock
  // detector that chose this transaction as a victim. AcquireWriteIntent
  // checks this on every entry; the next call returns false and the caller
  // aborts the transaction, releasing the held intent cleanly.
  std::atomic<bool> wounded_{false};
  // Set by TransactionManager::Abort when the undo walk begins; see
  // MarkAbortStarted for why waiters consult it.
  std::atomic<bool> abort_started_{false};
  // See DebugSetPhase (round 9 stall postmortem aid). Mutable: diagnostic
  // observers (and const-context park sites) tag the current phase.
  mutable std::atomic<const char*> debug_phase_{nullptr};
  std::atomic<int64_t> debug_tid_{0};
  // In-gate drain-budget exhaustion signal; see
  // NoteIndexReservationDrainExhausted. Mutable: the B+Tree drain points run
  // in const-adjacent contexts and Table consumes it from a const method.
  mutable std::atomic<uint32_t> index_drain_exhausted_{0};
  // Last write-intent acquisition failure reason (owner-thread confined).
  IntentWaitFailure intent_wait_failure_{IntentWaitFailure::kNone};
  friend class TransactionManager;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
