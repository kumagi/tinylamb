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

#include <atomic>
#include <optional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

enum class TransactionStatus : uint_fast8_t {
  kUnknown,
  kRunning,
  kCommitted,
  kAborted,
};

std::ostream& operator<<(std::ostream& o, const TransactionStatus& t);

class Transaction final {
 public:
  Transaction() = default;  // For test purpose only.
  Transaction(txn_id_t txn_id, TransactionManager* tm, bool read_only = false);
  Transaction(const Transaction& o) = delete;
  // Moving re-registers the transaction with its TransactionManager so a
  // Begin()-created object keeps its active_transactions_ entry valid at the
  // new address. The moved-from object is left manager-less.
  Transaction(Transaction&& o) noexcept;
  Transaction& operator=(const Transaction& o) = delete;
  Transaction& operator=(Transaction&& o) noexcept {
    if (this == &o) return *this;
    // This object may itself be registered under its old identity; drop that
    // entry before adopting the source's fields and registration slot.
    if (transaction_manager_ != nullptr) {
      transaction_manager_->UnregisterActiveTransaction(this);
    }
    txn_id_ = o.txn_id_;
    snapshot_ts_ = o.snapshot_ts_;
    read_set_ = std::move(o.read_set_);
    write_set_ = std::move(o.write_set_);
    mutated_index_roots_ = std::move(o.mutated_index_roots_);
    shard_epoch_ = o.shard_epoch_;
    write_epoch_.store(o.write_epoch_.load(std::memory_order_acquire),
                       std::memory_order_release);
    version_read_caches_ = std::move(o.version_read_caches_);
    read_state_mutex_ = std::move(o.read_state_mutex_);
    prev_lsn_ = o.prev_lsn_;
    status_ = o.status_;
    transaction_manager_ = o.transaction_manager_;
    read_only_ = o.read_only_;
    // Carry the victim flag: a Wound() issued right before the move would
    // otherwise be forgotten and the victim would keep running (and keep
    // its write intents) forever.
    wounded_.store(o.wounded_.load(std::memory_order_acquire),
                   std::memory_order_release);
    if (o.transaction_manager_ != nullptr) {
      o.transaction_manager_->MoveActiveTransaction(&o, this);
    }
    // Neutralize the moved-from object (PageRef-style): it no longer owns a
    // registry slot or manager services.
    o.transaction_manager_ = nullptr;
    return *this;
  }
  ~Transaction() = default;

  void SetStatus(TransactionStatus status);
  bool IsFinished() const {
    return status_ == TransactionStatus::kCommitted ||
           status_ == TransactionStatus::kAborted;
  }
  lsn_t PrevLSN() const { return prev_lsn_; }

  bool AddReadSet(const RowPosition& rp);
  bool AddWriteSet(const RowPosition& rp);
  // Like AddWriteSet, but installs `before` as the chain's base committed
  // version so concurrent readers keep seeing the old row until the write is
  // staged (prevents a phantom kNotExists during the intent window).
  bool AddWriteSet(const RowPosition& rp, std::string_view before);
  bool TryAddWriteSet(const RowPosition& rp);
  StatusOr<std::string_view> ReadVersion(
      const RowPosition& rp, std::optional<std::string_view> physical);
  void RegisterVersionWrite(const RowPosition& rp,
                            std::optional<std::string_view> before,
                            std::optional<std::string_view> after);

  [[nodiscard]] txn_id_t ID() const { return txn_id_; }
  [[nodiscard]] uint64_t SnapshotTimestamp() const { return snapshot_ts_; }
  [[nodiscard]] bool RequiresHistoricalRead() const;
  [[nodiscard]] bool IndexKeysMayBeStale() const;
  [[nodiscard]] bool IndexKeysMayBeStale(page_id_t index_root) const;
  // Index insertions need no stale-key fallback: MVCC hides rows newer than
  // the snapshot. Deletion/replacement can remove a key an old snapshot still
  // needs, so Table records those roots here and commit publishes their epoch.
  void MarkIndexKeysChanged(page_id_t index_root) {
    mutated_index_roots_.insert(index_root);
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

  Status PreCommit();
  void Abort();

  // Log the action. Returns LSN.
  lsn_t InsertLog(page_id_t pid, slot_t slot, std::string_view redo);
  lsn_t InsertLeafLog(page_id_t pid, std::string_view key,
                      std::string_view redo);
  lsn_t InsertBranchLog(page_id_t pid, std::string_view key, page_id_t redo);

  lsn_t UpdateLog(page_id_t pid, slot_t slot, std::string_view redo,
                  std::string_view undo);
  lsn_t UpdateLeafLog(page_id_t pid, std::string_view key,
                      std::string_view redo, std::string_view undo);
  lsn_t UpdateBranchLog(page_id_t pid, std::string_view key, page_id_t redo,
                        page_id_t undo);

  lsn_t DeleteLog(page_id_t pid, slot_t slot, std::string_view undo);
  lsn_t DeleteLeafLog(page_id_t pid, std::string_view key,
                      std::string_view undo);
  lsn_t DeleteBranchLog(page_id_t pid, std::string_view key, page_id_t undo);

  lsn_t SetLowestLog(page_id_t pid, page_id_t redo, page_id_t undo);

  lsn_t SetLowFence(page_id_t pid, const IndexKey& redo, const IndexKey& undo);
  lsn_t SetHighFence(page_id_t pid, const IndexKey& redo, const IndexKey& undo);
  lsn_t SetFoster(page_id_t pid, const FosterPair& redo,
                  const FosterPair& undo);

  lsn_t AllocatePageLog(page_id_t page_id, PageType new_page_type);

  lsn_t DestroyPageLog(page_id_t page_id);

  // Prepared mainly for testing.
  // Using this function is discouraged to get performance of flush pipelining.
  void CommitWait() const;

  PageManager* GetPageManager() {
    return transaction_manager_->GetPageManager();
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
  std::unordered_set<RowPosition> write_set_{};
  std::unordered_set<page_id_t> mutated_index_roots_{};
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
  std::unique_ptr<std::mutex> read_state_mutex_{
      std::make_unique<std::mutex>()};
  lsn_t prev_lsn_{};
  TransactionStatus status_ = TransactionStatus::kUnknown;
  bool read_only_{false};

  // Not owned by this class.
  TransactionManager* transaction_manager_{nullptr};
  // Set by an older transaction's Wound-Wait preemption or by a deadlock
  // detector that chose this transaction as a victim. AcquireWriteIntent
  // checks this on every entry; the next call returns false and the caller
  // aborts the transaction, releasing the held intent cleanly.
  std::atomic<bool> wounded_{false};
  friend class TransactionManager;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRANSACTION_HPP
