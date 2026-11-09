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
#include "common/status_or.hpp"
#include "page/page.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"

namespace tinylamb {

class Transaction;
class TransactionManager;

enum class TableTailResolution : uint8_t {
  kPublish,
  kDiscard,
  kPoison,
};

// Immutable sorted run in an LSM-Tree index.
struct LsmSortedRun {
  struct Entry {
    std::string key;
    std::string value;
    bool deleted{false};
    lsn_t committed_lsn{0};
  };
  uint64_t generation{0};
  std::vector<Entry> entries;

  [[nodiscard]] const Entry* Find(std::string_view key) const;
};

// Per-tree state for an LSM-Tree index or catalog.
struct LsmTreeState {
  struct MemEntry {
    std::string value;
    bool deleted{false};
    uint64_t generation{0};
    txn_id_t uncommitted_txn{0};
  };
  struct CommittedEntry {
    std::string value;
    bool deleted{false};
    lsn_t committed_lsn{0};
  };

  mutable std::shared_mutex mutex;
  uint64_t next_generation{1};
  size_t active_uncommitted_ops{0};
  bool destroyed{false};
  txn_id_t destroyed_by_txn{0};
  bool committed_destroyed{false};
  lsn_t committed_destroy_lsn{0};

  // Live MemTable (newest mutations, including uncommitted writes).
  std::map<std::string, MemEntry, std::less<>> memtable;
  // Committed staging buffer flushed into immutable SortedRuns.
  std::map<std::string, CommittedEntry, std::less<>> committed_mem;
  // Immutable SortedRuns ordered from oldest to newest.
  std::vector<LsmSortedRun> runs;

  static constexpr size_t kFlushThreshold = 1024;
  static constexpr size_t kCompactionThreshold = 4;

  [[nodiscard]] bool LookupLiveLocked(std::string_view key,
                                      std::string* value_out) const;
  bool FlushAndCompactIfIdleLocked();
  [[nodiscard]] LsmSortedRun BuildCompactedCommittedRunLocked() const;
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
  Status InitializeMetaPage() { return Status::kSuccess; }

  StatusOr<PageRef> AllocateNewPage(Transaction& txn, PageType new_page_type);
  Status DestroyPage(Transaction& txn, Page* target);

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
      bool ascending, txn_id_t reader_txn_id = 0) const;
  Status LsmDestroy(Transaction& txn, page_id_t tree_id);

  // Transaction lifecycle hooks (commit baseline promotion & in-memory undo).
  void ApplyTransactionCommit(txn_id_t txn_id, lsn_t commit_lsn,
                              const std::vector<LogRecord>& records);
  void ApplyTransactionAbort(txn_id_t txn_id,
                             const std::vector<LogRecord>& records);

  // Fuzzy checkpoint & fuzzy crash recovery.
  Status WriteFuzzyCheckpoint(lsn_t replay_start_lsn);
  Status LoadFuzzyCheckpoint(lsn_t* replay_start_lsn_out,
                             bool* loaded_out = nullptr);
  Status ReplayCommittedRecord(lsn_t lsn, const LogRecord& record);
  void SetCompactionCallback(std::function<void()> callback);
  [[nodiscard]] txn_id_t MaxCommittedTxnId() const noexcept {
    return max_committed_txn_id_.load(std::memory_order_acquire);
  }
  void DropAllState();
  void DropAllPages() { DropAllState(); }
  Status FlushPageForTest(page_id_t /*pid*/) { return WriteFuzzyCheckpoint(0); }
  PageManager* GetPool() { return this; }
  void CloseDatabaseFile();

  [[nodiscard]] const std::string& DbPath() const { return db_name_; }
  [[nodiscard]] uint64_t CacheHits() const noexcept {
    return cache_hits_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t CacheMisses() const noexcept {
    return cache_misses_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t PinnedPageCount() const noexcept { return 0; }
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

  std::string db_name_;
  mutable std::mutex checkpoint_io_mu_;
  int lock_fd_{-1};

  mutable std::mutex blocks_mu_;
  std::unordered_map<page_id_t, std::unique_ptr<Page>> blocks_;
  page_id_t max_page_id_{10};
  page_id_t committed_max_page_id_{10};
  std::vector<page_id_t> free_pages_;
  std::unordered_map<txn_id_t, std::vector<page_id_t>> pending_destroyed_pages_;

  mutable std::mutex trees_mu_;
  mutable std::unordered_map<page_id_t, std::unique_ptr<LsmTreeState>>
      lsm_trees_;

  static constexpr size_t kCompactionPersistOpsThreshold = 1048576;

  std::atomic<txn_id_t> max_committed_txn_id_{0};
  std::atomic<size_t> unpersisted_committed_ops_{0};
  mutable std::mutex compaction_cb_mu_;
  std::function<void()> compaction_cb_;

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
