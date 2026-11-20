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

#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/status_or.hpp"
#include "page/page.hpp"
#include "page/page_lsm_store.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"

namespace tinylamb {

class Transaction;
class TransactionManager;
class SpillStore;

enum class TableTailResolution : uint8_t {
  kPublish,
  kDiscard,
  kPoison,
};

// Immutable disk-backed sorted run in an LSM-Tree index (payload lives in
// `<db>.lsm.<gen>`; RAM retains only sparse block fence metadata).
struct LsmSortedRun {
  using Entry = LsmRunEntry;
  uint64_t generation{0};
  uint64_t run_id{0};
  uint64_t entry_count{0};
  // Computed by the stream writer but currently unused by readers (no
  // tombstone-priority compaction policy yet); do not trust it for decisions
  // until a reader is wired up and verified.
  bool has_tombstones{false};
  std::vector<LsmRunBlockMeta> blocks;
};

// Per-tree state for an LSM-Tree index or catalog.
struct LsmTreeState {
  struct MemEntry {
    std::string value;
    bool deleted{false};
    uint64_t generation{0};
    // T4580: a single "latest uncommitted txn" scalar was removed here --
    // it was written at five sites, maintained by the reclaim helpers'
    // out-parameter, and read nowhere. uncommitted_owners is the sole
    // visibility bookkeeping (see has_foreign_uncommitted below it).
    std::vector<txn_id_t> uncommitted_owners{};
  };
  struct CommittedEntry {
    std::string value;
    bool deleted{false};
    lsn_t committed_lsn{0};
  };

  mutable std::shared_mutex mutex;
  // Version allocator AND revalidation epoch: every transition that can
  // change what LsmLookupLiveLocked returns for any key (memtable mutation,
  // commit promotion, rollback erase, flush move, clear-all, destroyed
  // flips) bumps it.  The LsmInsert/LsmUpdate/LsmDelete two-phase writers
  // capture it under the shared probe latch and re-run the probe when it
  // moved (P3834/386).  All bumps happen under `mutex`.
  uint64_t next_generation{1};
  uint64_t destroy_epoch{0};
  size_t active_uncommitted_ops{0};
  bool destroyed{false};
  txn_id_t destroyed_by_txn{0};
  bool committed_destroyed{false};
  lsn_t committed_destroy_lsn{0};
  bool flushing_immutable{false};
  bool compacting{false};
  size_t accounted_bytes{0};
  size_t accounted_emergency_bytes{0};
  size_t committed_mem_bytes{0};
  size_t memtable_bytes{0};
  size_t immutable_mem_bytes{0};
  size_t runs_bytes{0};

  // Live MemTable (only active uncommitted mutations).
  std::map<std::string, MemEntry, std::less<>> memtable;
  // Committed staging buffer flushed into immutable disk-backed SortedRuns.
  std::map<std::string, CommittedEntry, std::less<>> committed_mem;
  // Frozen snapshot of `committed_mem` currently streaming to `<db>.lsm`.
  std::map<std::string, CommittedEntry, std::less<>> immutable_mem;
  // Immutable SortedRuns ordered from oldest to newest.
  std::vector<LsmSortedRun> runs;

  static constexpr size_t kCompactionThreshold = 4;

  static size_t CommittedEntryBytes(std::string_view key,
                                    const CommittedEntry& c) noexcept {
    return key.size() + c.value.size() + 64;
  }
  static size_t MemEntryBytes(std::string_view key,
                              const MemEntry& m) noexcept {
    return key.size() + m.value.size() +
           (m.uncommitted_owners.capacity() * sizeof(txn_id_t)) + 96;
  }
  static size_t RunBytes(const LsmSortedRun& r) noexcept {
    size_t bytes =
        sizeof(LsmSortedRun) + (r.blocks.size() * sizeof(LsmRunBlockMeta));
    for (const LsmRunBlockMeta& bm : r.blocks) {
      bytes += bm.last_key.size();
    }
    return bytes;
  }

  void SetCommittedEntryLocked(std::string key, CommittedEntry entry) {
    ++next_generation;  // lookup-visible (P3834/386)
    auto [it, inserted] =
        committed_mem.try_emplace(std::move(key), std::move(entry));
    if (inserted) {
      committed_mem_bytes += CommittedEntryBytes(it->first, it->second);
    } else {
      committed_mem_bytes -= CommittedEntryBytes(it->first, it->second);
      it->second = std::move(entry);
      committed_mem_bytes += CommittedEntryBytes(it->first, it->second);
    }
  }

  template <typename Fn>
  void MutateMemtableEntryLocked(std::string_view key, Fn&& fn) {
    ++next_generation;  // lookup-visible (P3834/386)
    auto it = memtable.find(key);
    if (it == memtable.end()) {
      it = memtable.emplace(std::string(key), MemEntry{}).first;
    } else {
      memtable_bytes -= MemEntryBytes(it->first, it->second);
    }
    std::forward<Fn>(fn)(it->second);
    memtable_bytes += MemEntryBytes(it->first, it->second);
  }

  template <typename Fn>
  void UpdateOrEraseMemtableEntryLocked(std::string_view key, Fn&& fn) {
    auto it = memtable.find(key);
    if (it == memtable.end()) {
      return;
    }
    ++next_generation;  // erase/owner change is lookup-visible (P3834/386)
    memtable_bytes -= MemEntryBytes(it->first, it->second);
    if (fn(it->second)) {
      memtable_bytes += MemEntryBytes(it->first, it->second);
    } else {
      memtable.erase(it);
    }
  }

  void MoveCommittedToImmutableLocked() noexcept {
    ++next_generation;  // flush move + run-gen allocation (P3834/386)
    immutable_mem = std::move(committed_mem);
    immutable_mem_bytes = committed_mem_bytes;
    committed_mem.clear();
    committed_mem_bytes = 0;
  }

  void ClearImmutableLocked() noexcept {
    ++next_generation;  // entries leave the queried maps (P3834/386)
    immutable_mem.clear();
    immutable_mem_bytes = 0;
  }

  void RestoreImmutableToCommittedLocked() {
    for (auto& [k, c] : immutable_mem) {
      // Newer committed entries win: an existing key is not overwritten by
      // an older immutable snapshot during rollback.
      auto [it, inserted] = committed_mem.try_emplace(k, std::move(c));
      if (inserted) {
        committed_mem_bytes += CommittedEntryBytes(it->first, it->second);
      }
    }
    ClearImmutableLocked();
  }

  void AppendRunLocked(LsmSortedRun run) {
    runs_bytes += RunBytes(run);
    runs.push_back(std::move(run));
  }

  void ReplaceRunPrefixLocked(size_t prefix_count,
                              std::optional<LsmSortedRun> replacement) {
    for (size_t i = 0; i < prefix_count; ++i) {
      runs_bytes -= RunBytes(runs[i]);
    }
    runs.erase(runs.begin(),
               runs.begin() + static_cast<ptrdiff_t>(prefix_count));
    if (replacement.has_value() && !replacement->blocks.empty()) {
      runs_bytes += RunBytes(*replacement);
      runs.insert(runs.begin(), std::move(*replacement));
    }
  }

  void ClearAllEntriesLocked() noexcept {
    ++next_generation;  // everything leaves the queried maps (P3834/386)
    memtable.clear();
    memtable_bytes = 0;
    committed_mem.clear();
    committed_mem_bytes = 0;
    immutable_mem.clear();
    immutable_mem_bytes = 0;
    runs.clear();
    runs_bytes = 0;
  }

  [[nodiscard]] size_t EstimateCommittedMemBytesLocked() const noexcept {
    return committed_mem_bytes;
  }
  [[nodiscard]] size_t EstimateTotalMemoryBytesLocked() const noexcept {
    return committed_mem_bytes + memtable_bytes + immutable_mem_bytes +
           runs_bytes;
  }
};

// LSM Block & Tree Storage Manager.
// Manages unbounded row blocks for tables and LSM-Trees for indexes/catalogs,
// along with fuzzy checkpointing and commit-filtered fuzzy recovery.
class PageManager {
 public:
  static StatusOr<std::unique_ptr<PageManager>> Create(
      std::string_view db_name, size_t capacity = 0,
      bool initialize_meta = true);
  PageManager(const PageManager&) = delete;
  PageManager& operator=(const PageManager&) = delete;
  PageManager(PageManager&&) = delete;
  PageManager& operator=(PageManager&&) = delete;
  ~PageManager();

  StatusOr<PageRef> GetPage(page_id_t page_id, bool shared = false);
  static Status InitializeMetaPage() { return Status::kSuccess; }

  StatusOr<PageRef> AllocateNewPage(Transaction& txn, PageType new_page_type);
  Status DestroyPage(Transaction& txn, Page* target);
  StatusOr<std::vector<page_id_t>> InstallColumnarStubChain(
      page_id_t first_pid, const std::vector<uint32_t>& block_row_counts,
      lsn_t commit_lsn);
  StatusOr<std::vector<page_id_t>> ReplaceTableChainWithColumnarStubs(
      page_id_t first_pid, const std::vector<page_id_t>& old_pids,
      const std::vector<uint32_t>& block_row_counts, lsn_t commit_lsn);
  Status HydrateColumnarStubPage(page_id_t page_id,
                                 std::vector<std::string>&& encoded_rows);

  // Table tail coordination for concurrent appends.
  page_id_t GetTableTail(page_id_t first_page, page_id_t catalog_hint);
  page_id_t GetTableTailFor(txn_id_t viewer, page_id_t first_page,
                            page_id_t catalog_hint);
  Status TryReserveTableTail(txn_id_t owner, page_id_t first_page,
                             page_id_t expected);
  Status ReserveTableTail(txn_id_t owner, page_id_t first_page,
                          page_id_t expected);
  Status ReserveTableTail(Transaction& txn, page_id_t first_page,
                          page_id_t expected);
  Status WaitForTableTailPage(txn_id_t viewer, page_id_t first_page,
                              page_id_t page_id);
  Status WaitForTableTailPage(Transaction& txn, page_id_t first_page,
                              page_id_t page_id);
  Status LinkReservedTableTail(txn_id_t owner, page_id_t first_page,
                               page_id_t predecessor, page_id_t successor);
  // Snapshot readers only: true when |page_id| is an unpublished tail page
  // of |first_page| owned by another transaction that has not committed.
  // Everything on (and after) such a page is that owner's uncommitted
  // growth, invisible to |viewer|'s snapshot, so a scan ends the chain there
  // instead of blocking on the owner (who may stay open for minutes).
  // Writers that must append past the tail keep using WaitForTableTailPage.
  [[nodiscard]] bool TailPageInvisibleToSnapshot(const Transaction& viewer,
                                                 page_id_t first_page,
                                                 page_id_t page_id);
  Status CheckTableTailPage(txn_id_t viewer, page_id_t first_page,
                            page_id_t page_id);
  Status WaitForTableTailQuiescence(page_id_t first_page, txn_id_t requester);
  void ResolveTableTailReservations(txn_id_t owner,
                                    TableTailResolution resolution);
  void CancelTableTailReservation(txn_id_t owner, page_id_t first_page);
  void AdvanceTableTail(page_id_t first_page, page_id_t expected,
                        page_id_t next);
  void ForgetTableTail(page_id_t first_page);
  void NotifyTableTailWaiters();

  // LSM-Tree Index operations (arbitrary key and value sizes).
  Status LsmInsert(Transaction& txn, page_id_t tree_id, std::string_view key,
                   std::string_view value);
  Status LsmUpdate(Transaction& txn, page_id_t tree_id, std::string_view key,
                   std::string_view value);
  Status LsmDelete(Transaction& txn, page_id_t tree_id, std::string_view key);
  StatusOr<std::string> LsmRead(page_id_t tree_id, std::string_view key) const;
  std::vector<std::pair<std::string, std::string>> LsmScanRange(
      page_id_t tree_id, std::string_view begin, std::string_view end,
      bool ascending, const Transaction* reader = nullptr,
      Status* status_out = nullptr) const;
  Status LsmDestroy(Transaction& txn, page_id_t tree_id);

  // Transaction lifecycle hooks (commit baseline promotion & in-memory undo).
  struct TransactionCommitState {
    page_id_t last_lsm_tree{0};
    bool any_lsm_commit{false};
    size_t applied_count{0};
  };

  void BeginTransactionCommit(txn_id_t txn_id);
  Status ApplyTransactionCommitRecord(txn_id_t txn_id, lsn_t commit_lsn,
                                      const LogRecord& r,
                                      TransactionCommitState* state);
  void FinishTransactionCommit(txn_id_t txn_id,
                               const TransactionCommitState& state);
  Status ApplyTransactionCommit(txn_id_t txn_id, lsn_t commit_lsn,
                                const std::vector<LogRecord>& records);

  void BeginTransactionAbort(txn_id_t txn_id);
  void ApplyTransactionAbortRecord(txn_id_t txn_id, const LogRecord& r);
  void FinishTransactionAbort(txn_id_t txn_id);
  void ApplyTransactionAbort(txn_id_t txn_id,
                             const std::vector<LogRecord>& records);

  // Fuzzy checkpoint & fuzzy crash recovery.
  Status WriteFuzzyCheckpoint(lsn_t replay_start_lsn);
  Status LoadFuzzyCheckpoint(lsn_t* replay_start_lsn_out,
                             bool* loaded_out = nullptr);
  Status ReplayCommittedRecord(lsn_t lsn, const LogRecord& record);
  void SetCompactionCallback(std::function<void()> callback);
  void SetDurableWaiter(std::function<Status(lsn_t)> waiter);
  using ColumnarStubHydrator =
      std::function<StatusOr<std::vector<std::string>>(page_id_t)>;
  void SetColumnarStubHydrator(ColumnarStubHydrator hydrator);
  void SetRecoveryDestroyFloorLsns(std::unordered_map<page_id_t, lsn_t> floors);
  void ClearRecoveryDestroyFloorLsns();
  void SetBufferPoolLimitBytesForTest(size_t limit_bytes) {
    test_buffer_pool_limit_bytes_.store(limit_bytes, std::memory_order_release);
  }
  Status EvictColdPagesForTest(size_t target_bytes);
  [[nodiscard]] size_t ResidentBytes() const noexcept {
    return resident_bytes_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t EvictedPageCount() const noexcept {
    return evicted_page_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t EvictedDirBytes() const noexcept {
    std::scoped_lock lk(blocks_mu_);
    return evicted_pages_.Bytes();
  }
  [[nodiscard]] size_t UncommittedSpillCount() const noexcept {
    std::scoped_lock lk(blocks_mu_);
    return uncommitted_spills_.size();
  }
  [[nodiscard]] uint64_t UncommittedSpillEvictions() const noexcept {
    return uncommitted_spill_evictions_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t UncommittedSpillFaults() const noexcept {
    return uncommitted_spill_faults_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t TotalLsmWriteBufferBytes() const noexcept {
    return total_lsm_mem_bytes_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t LsmBlockCacheBytes() const noexcept {
    return lsm_cache_.CachedBytes();
  }
  [[nodiscard]] txn_id_t MaxCommittedTxnId() const noexcept {
    return max_committed_txn_id_.load(std::memory_order_acquire);
  }
  void DropAllState();
  void DropAllPages() { DropAllState(); }
  Status FlushAllPagesForTest() { return WriteFuzzyCheckpoint(0); }
  void FlushLsmBuffersForTest(page_id_t hint_tree_id) {
    MaybeFlushLsmWriteBuffers(hint_tree_id);
  }
  // Historical alias that returns this PageManager (PagePool is retired).
  PageManager* GetPool() { return this; }
  void CloseDatabaseFile();

  [[nodiscard]] const std::string& DbPath() const { return db_name_; }
  [[nodiscard]] uint64_t CacheHits() const noexcept {
    return cache_hits_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t CacheMisses() const noexcept {
    return cache_misses_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] static size_t PinnedPageCount() noexcept { return 0; }
  [[nodiscard]] uint64_t TailWaitCount() const noexcept {
    return tail_wait_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t TailWaitNs() const noexcept {
    return tail_wait_ns_.load(std::memory_order_relaxed);
  }

  friend std::ostream& operator<<(std::ostream& o, const PageManager& pm) {
    o << "PageManager(db=" << pm.db_name_ << ")";
    return o;
  }

 private:
  PageManager(std::string db_name, int lock_fd);

  LsmTreeState* GetOrCreateTreeLocked(page_id_t tree_id) const;
  LsmTreeState* FindTreeLocked(page_id_t tree_id) const;

  Status ReserveTableTailImpl(txn_id_t owner, page_id_t first_page,
                              page_id_t expected, const Transaction* watcher);
  Status WaitForTableTailPageImpl(txn_id_t viewer, page_id_t first_page,
                                  page_id_t page_id,
                                  const Transaction* watcher);

  [[nodiscard]] size_t EffectiveBufferPoolLimit() const noexcept;
  [[nodiscard]] size_t EffectiveLsmWriteBufferLimit() const noexcept;
  [[nodiscard]] size_t EffectiveLsmBlockCacheLimit() const noexcept;
  [[nodiscard]] std::string BlocksFilePath(uint64_t generation) const;
  [[nodiscard]] std::string LsmFilePath(uint64_t generation) const;
  void AccountPageMemoryLocked(Page& p) noexcept;
  void ReleasePageMemoryOnEraseLocked(Page& p) noexcept;
  void AccountEvictedDirDeltaLocked(ssize_t bytes_delta) noexcept;
  void ReleaseAllAccountedMemory() noexcept;
  void NotePageTouchedLocked(Page& p);
  void CloseBlocksFileLocked();
  void CloseLsmFileLocked();
  void CleanupStaleBlocksFiles(uint64_t keep_generation) const;
  void CleanupStaleLsmFiles(uint64_t keep_generation) const;
  Status OpenOrCreateBlocksFileLocked(uint64_t generation, bool truncate,
                                      uint64_t* file_size_out = nullptr);
  Status OpenOrCreateLsmFileLocked(uint64_t generation, bool truncate,
                                   uint64_t* file_size_out = nullptr) const;
  StatusOr<std::shared_ptr<Page>> EnsureEvictedPageResident(
      page_id_t pid, const EvictedPageDesc& desc);
  Status EnsurePidResident(page_id_t pid);
  [[nodiscard]] bool IsRecoveryDestroyFloorCandidate(page_id_t pid) const;
  [[nodiscard]] bool ShouldSkipRecoveryByDestroyFloorLocked(page_id_t pid,
                                                            lsn_t lsn) const;
  Status EnsureRecoveryStubHydrated(page_id_t pid, lsn_t lsn, bool* skip_out);
  void MaybeEvictIfOverBudget(size_t target_bytes);
  size_t TryEvictForReclaim(size_t bytes_needed) noexcept;
  Status EvictBatch(size_t target_bytes, bool try_locks_only = false);
  Status WaitForDurableLSN(lsn_t lsn);
  void AccountTreeMemoryLocked(LsmTreeState& tree) noexcept;
  void ReleaseAllTreeMemoryLocked() noexcept;
  StatusOr<bool> LsmLookupLiveLocked(const LsmTreeState& tree,
                                     std::string_view key,
                                     std::string* value_out) const;
  StatusOr<bool> LsmLookupCommittedLocked(const LsmTreeState& tree,
                                          std::string_view key,
                                          std::string* value_out,
                                          lsn_t* lsn_out = nullptr) const;
  Status FlushTreeCommittedMemToDisk(page_id_t tree_id, LsmTreeState* tree);
  Status CompactTreeRunsToDisk(page_id_t tree_id, LsmTreeState* tree,
                               bool drop_tombstones);
  void MaybeFlushLsmWriteBuffers(page_id_t hint_tree_id);

  // LSM file rotation barrier (checkpoint safety).  A flush or compaction
  // stream appends to `lsm_fd_` across many blocks, dropping and re-taking
  // `lsm_file_mu_` between blocks; checkpoint rotation swaps that fd and
  // deletes the old generation file.  Without serialization a run streamed
  // into the old file ends up with block offsets that no longer resolve
  // against the live fd (wrong bytes or IO failure).  Every writer that
  // streams into the LSM file holds `LsmStreamGuard` from before its first
  // tree-latch acquisition until its run registration completes; rotation
  // sets `lsm_rotating_`, waits for the user count to drain, then swaps.
  // Latch order is always stream guard -> tree mutex (never the reverse).
  class LsmStreamGuard {
   public:
    explicit LsmStreamGuard(PageManager* pm) : pm_(pm) {
      pm_->BeginLsmStream();
    }
    ~LsmStreamGuard() { pm_->EndLsmStream(); }
    LsmStreamGuard(const LsmStreamGuard&) = delete;
    LsmStreamGuard& operator=(const LsmStreamGuard&) = delete;
    LsmStreamGuard(LsmStreamGuard&&) = delete;
    LsmStreamGuard& operator=(LsmStreamGuard&&) = delete;

   private:
    PageManager* pm_;
  };
  void BeginLsmStream() {
    std::unique_lock lk(lsm_file_mu_);
    lsm_stream_cv_.wait(lk, [this] { return !lsm_rotating_; });
    ++lsm_stream_users_;
  }
  void EndLsmStream() {
    {
      std::scoped_lock lk(lsm_file_mu_);
      --lsm_stream_users_;
    }
    lsm_stream_cv_.notify_all();
  }
  // Checkpoint-side counterpart of `LsmStreamGuard`: held from before the
  // per-tree run snapshot until the new generation's fd swap, run-list
  // replacement and stale-file removal are done.  While held, no flush or
  // compaction can stream into or register against the old fd, so every run
  // still referenced after the swap lives in exactly one (open) file.
  class LsmRotationGuard {
   public:
    explicit LsmRotationGuard(PageManager* pm) : pm_(pm) {
      std::unique_lock lk(pm_->lsm_file_mu_);
      pm_->lsm_rotating_ = true;
      pm_->lsm_stream_cv_.wait(lk, [pm] { return pm->lsm_stream_users_ == 0; });
    }
    ~LsmRotationGuard() {
      {
        std::scoped_lock lk(pm_->lsm_file_mu_);
        pm_->lsm_rotating_ = false;
      }
      pm_->lsm_stream_cv_.notify_all();
    }
    LsmRotationGuard(const LsmRotationGuard&) = delete;
    LsmRotationGuard& operator=(const LsmRotationGuard&) = delete;
    LsmRotationGuard(LsmRotationGuard&&) = delete;
    LsmRotationGuard& operator=(LsmRotationGuard&&) = delete;

   private:
    PageManager* pm_;
  };

  std::string db_name_;
  mutable std::mutex checkpoint_io_mu_;
  int lock_fd_{-1};

  mutable std::mutex blocks_mu_;
  std::map<page_id_t, std::shared_ptr<Page>> blocks_;
  EvictedPageTable evicted_pages_;
  size_t evicted_dir_emergency_bytes_{0};
  page_id_t max_page_id_{10};
  page_id_t committed_max_page_id_{10};
  BoundedFreeIdSet free_ids_;
  std::unordered_map<txn_id_t, std::vector<page_id_t>> pending_destroyed_pages_;

  mutable std::mutex eviction_mu_;
  std::deque<page_id_t> eviction_fifo_;
  std::atomic<size_t> resident_bytes_{0};
  std::atomic<size_t> resident_emergency_bytes_{0};
  std::atomic<size_t> test_buffer_pool_limit_bytes_{0};
  std::atomic<size_t> evicted_page_count_{0};
  std::unique_ptr<SpillStore> uncommitted_spill_store_;
  mutable std::mutex uncommitted_spill_store_mu_;
  std::unordered_map<page_id_t, uint64_t> uncommitted_spills_;
  std::atomic<uint64_t> uncommitted_spill_evictions_{0};
  std::atomic<uint64_t> uncommitted_spill_faults_{0};
  [[nodiscard]] SpillStore* GetOrCreateUncommittedSpillStore();
  void DropUncommittedSpillSegment(uint64_t seg_id) noexcept;
  struct ReclaimBridge {
    std::mutex mu;
    PageManager* pm{nullptr};
  };
  std::shared_ptr<ReclaimBridge> reclaim_bridge_;
  ScopedReclaimHook reclaim_hook_;

  struct ManagedBlockFile {
    ManagedBlockFile() = default;
    ManagedBlockFile(int fd_in, uint64_t gen_in)
        : fd(fd_in), generation(gen_in) {}
    ManagedBlockFile(const ManagedBlockFile&) = delete;
    ManagedBlockFile& operator=(const ManagedBlockFile&) = delete;
    ManagedBlockFile(ManagedBlockFile&&) = delete;
    ManagedBlockFile& operator=(ManagedBlockFile&&) = delete;
    ~ManagedBlockFile();
    int fd{-1};
    uint64_t generation{0};
  };
  [[nodiscard]] std::shared_ptr<const ManagedBlockFile> GetBlockFileHandle(
      uint64_t generation) const;

  mutable std::mutex blocks_file_mu_;
  int blocks_fd_{-1};
  uint64_t blocks_generation_{0};
  uint64_t blocks_append_offset_{0};
  std::shared_ptr<const ManagedBlockFile> active_block_file_;
  mutable std::unordered_map<uint64_t, std::shared_ptr<const ManagedBlockFile>>
      retained_block_files_;

  mutable std::mutex lsm_file_mu_;
  mutable int lsm_fd_{-1};
  mutable uint64_t lsm_generation_{0};
  mutable uint64_t lsm_append_offset_{0};
  bool lsm_rotating_{false};
  uint32_t lsm_stream_users_{0};
  std::condition_variable lsm_stream_cv_;
  mutable std::atomic<uint64_t> next_lsm_run_id_{1};
  mutable LsmBlockCache lsm_cache_;
  mutable std::atomic<size_t> total_lsm_mem_bytes_{0};
  mutable std::atomic<size_t> total_lsm_mem_emergency_bytes_{0};

  mutable std::mutex trees_mu_;
  mutable std::unordered_map<page_id_t, std::unique_ptr<LsmTreeState>>
      lsm_trees_;

  static constexpr size_t kCompactionPersistOpsThreshold = 131072;

  std::atomic<txn_id_t> max_committed_txn_id_{0};
  std::atomic<size_t> unpersisted_committed_ops_{0};
  mutable std::mutex compaction_cb_mu_;
  std::function<void()> compaction_cb_;
  mutable std::mutex durable_waiter_mu_;
  std::function<Status(lsn_t)> durable_waiter_;
  mutable std::mutex stub_hydrator_mu_;
  ColumnarStubHydrator stub_hydrator_;
  std::unordered_map<page_id_t, lsn_t> recovery_destroy_floors_;

  mutable std::atomic<uint64_t> cache_hits_{0};
  mutable std::atomic<uint64_t> cache_misses_{0};

  struct TailReservation {
    txn_id_t owner{0};
    page_id_t predecessor{0};
    page_id_t new_tail{0};
  };
  struct PendingTailLink {
    txn_id_t owner{0};
    page_id_t first_page{0};
    page_id_t predecessor{0};
    bool poisoned{false};
  };
  std::mutex table_tails_mu_;
  std::condition_variable table_tails_cv_;
  std::unordered_map<page_id_t, page_id_t> table_tails_;
  std::unordered_map<page_id_t, TailReservation> tail_reservations_;
  std::unordered_map<page_id_t, PendingTailLink> unpublished_tail_pages_;
  std::atomic<uint64_t> tail_wait_count_{0};
  std::atomic<uint64_t> tail_wait_ns_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
