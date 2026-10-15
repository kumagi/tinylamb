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
#include <cstdint>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "common/log_message.hpp"
#include "page/page.hpp"
#include "page/page_pool.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

class Logger;
class RecoveryManager;
class MetaPage;
class Transaction;
class TransactionManager;

enum class TableTailResolution : uint8_t {
  kPublish,
  kDiscard,
  kPoison,
};

class PageManager {
  static constexpr page_id_t kMetaPageId = 0;

 public:
  // Opens the backing page file and initializes the meta page.
  static StatusOr<std::unique_ptr<PageManager>> Create(
      std::string_view db_name, size_t capacity, bool initialize_meta = true);
  PageManager(const PageManager&) = delete;
  PageManager& operator=(const PageManager&) = delete;
  PageManager(PageManager&&) = delete;
  PageManager& operator=(PageManager&&) = delete;
  ~PageManager() = default;

  StatusOr<PageRef> GetPage(page_id_t page_id, bool shared = false);
  Status InitializeMetaPage();

  StatusOr<PageRef> AllocateNewPage(Transaction& txn, PageType new_page_type);

  Status ReplayAllocation(PageRef& target, lsn_t lsn, const LogRecord& log,
                          bool redo_target);

  StatusOr<lsn_t> CompensateAllocation(PageRef& target, lsn_t lsn,
                                       const LogRecord& log,
                                       TransactionManager* tm);

  StatusOr<std::string> GetAllocationMetaPayload();

  // Table metadata on disk records a conservative append starting point.
  // Keep the live tail per table in the PageManager so independently decoded
  // Table objects do not rescan the complete row-page chain on every insert.
  page_id_t GetTableTail(page_id_t first_page, page_id_t catalog_hint);
  page_id_t GetTableTailFor(txn_id_t viewer, page_id_t first_page,
                            page_id_t catalog_hint);
  Status TryReserveTableTail(txn_id_t owner, page_id_t first_page,
                             page_id_t expected);
  Status ReserveTableTail(txn_id_t owner, page_id_t first_page,
                          page_id_t expected);
  // Transaction-aware tail waits. A caller that already holds a tail
  // reservation on a DIFFERENT table must never wait for a second one: two
  // growers each holding the other's wanted table would both ride the whole
  // wait limit and then fail -- a reservation cycle the intent deadlock
  // detector cannot see, because the wait-for graph only tracks write
  // intents. A wounded transaction must stop waiting so its abort unwinds
  // promptly instead of holding its row intents through the full limit.
  // The txn_id_t overloads keep watcher-less behavior.
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

  // DROP TABLE (D3): the table's pages go back to the allocator, so its
  // cached tail hint must disappear with them; a later table that reuses the
  // first page id must not inherit a stale tail.
  void ForgetTableTail(page_id_t first_page);

  // D3 (docs/design.md): undo of a page destroy restores the page image and
  // pops it from the allocator free stack (see MetaPage::PopFreePageHead).
  // Returns an error instead of aborting when the meta page is unreadable:
  // callers run on the recovery/abort path, exactly when the meta page is
  // most likely to be distressed.  Defined in the .cpp because it needs the
  // full Page definition.
  Status PopFreePageHead(page_id_t pid, page_id_t next);

  // Logically delete the page.
  Status DestroyPage(Transaction& txn, Page* target);

  PagePool* GetPool() { return pool_.get(); }

  [[nodiscard]] uint64_t CacheHits() const noexcept {
    return pool_ ? pool_->CacheHits() : 0;
  }
  [[nodiscard]] uint64_t CacheMisses() const noexcept {
    return pool_ ? pool_->CacheMisses() : 0;
  }
  [[nodiscard]] size_t PinnedPageCount() const noexcept {
    return pool_ ? pool_->PinnedPageCount() : 0;
  }
  // Sampled tail-wait accounting (>1ms waits only; the uncontended majority
  // records nothing). A climbing total with a low count means single long
  // stalls (commit-coupled tail growth); a high count means convoy.
  [[nodiscard]] uint64_t TailWaitCount() const noexcept {
    return tail_wait_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] uint64_t TailWaitNs() const noexcept {
    return tail_wait_ns_.load(std::memory_order_relaxed);
  }

  friend std::ostream& operator<<(std::ostream& o, const PageManager& pm) {
    o << "PageManager(pool=" << *pm.pool_ << ")";
    return o;
  }

 private:
  PageManager(std::string_view db_name, size_t capacity,
              std::unique_ptr<PagePool> pool);

  StatusOr<PageRef> GetMetaPage();
  Status ApplyAllocationState(PageRef& target, lsn_t rec_lsn, lsn_t page_lsn,
                              const LogRecord& log, bool redo_target);

  Status ReserveTableTailImpl(txn_id_t owner, page_id_t first_page,
                              page_id_t expected, Transaction* watcher);
  Status WaitForTableTailPageImpl(txn_id_t viewer, page_id_t first_page,
                                  page_id_t page_id, Transaction* watcher);
  [[nodiscard]] bool OwnerHoldsForeignTailReservation(
      txn_id_t owner, page_id_t first_page) const;

  friend class RecoveryManager;

  struct PendingTableTail {
    txn_id_t owner = 0;
    page_id_t owner_tail = 0;
    std::unordered_set<page_id_t> unpublished_pages;
  };

  struct TableTailState {
    explicit TableTailState(page_id_t tail) : published_tail(tail) {}
    page_id_t published_tail = 0;
    std::optional<PendingTableTail> pending;
    bool poisoned = false;
  };

  std::unique_ptr<PagePool> pool_;
  std::mutex table_tails_mu_;
  std::condition_variable table_tails_cv_;
  std::unordered_map<page_id_t, TableTailState> table_tails_;
  std::unordered_map<txn_id_t, std::unordered_set<page_id_t>>
      table_tail_reservations_;
  mutable std::atomic<uint64_t> tail_wait_count_{0};
  mutable std::atomic<uint64_t> tail_wait_ns_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
