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

#include "page_manager.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string_view>

#include "common/constants.hpp"
#include "page/meta_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

StatusOr<std::unique_ptr<PageManager>> PageManager::Create(
    std::string_view db_name, size_t capacity) {
  ASSIGN_OR_RETURN(std::unique_ptr<PagePool>, pool,
                   PagePool::Create(db_name, capacity));
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  auto manager = std::unique_ptr<PageManager>(
      new PageManager(db_name, capacity, std::move(pool)));  // NOLINT
  // Initialize the meta page (idempotent: existing images are kept).
  RETURN_IF_FAIL(manager->GetMetaPage().GetStatus());
  return manager;
}

PageManager::PageManager(std::string_view /*db_name*/, size_t /*capacity*/,
                         std::unique_ptr<PagePool> pool)
    : pool_(std::move(pool)) {}

StatusOr<PageRef> PageManager::GetPage(uint64_t page_id, bool shared) {
  bool cache_hit = false;
  ASSIGN_OR_RETURN(PageRef, ref, pool_->GetPage(page_id, &cache_hit, shared));
  if (!cache_hit && !ref->IsValid()) {
    // Found a broken or new page: hand back the empty PageRef the existing
    // callers (BPlusTree bootstrap) test with IsValid().
    return PageRef{};
  }
  return ref;
}

page_id_t PageManager::GetTableTail(page_id_t first_page,
                                    page_id_t catalog_hint) {
  std::scoped_lock lock(table_tails_mu_);
  return table_tails_.try_emplace(first_page, catalog_hint).first->second;
}

void PageManager::AdvanceTableTail(page_id_t first_page, page_id_t expected,
                                   page_id_t next) {
  std::scoped_lock lock(table_tails_mu_);
  auto [it, inserted] = table_tails_.try_emplace(first_page, next);
  if (!inserted && it->second == expected) {
    it->second = next;
  }
}

// Logically delete the page.
Status PageManager::DestroyPage(Transaction& system_txn, Page* target) {
  ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
  return meta->DestroyPage(system_txn, target);
}

// D3 (docs/design.md): undo restored the destroyed page; drop it from the
// allocator free stack so the next AllocateNewPage cannot re-issue it.
// Reports the meta-page failure instead of aborting: callers run on the
// recovery/abort path, where MoveValue()'s LOG(FATAL)+abort would turn a
// distressed meta page into a whole-process crash.
//
// The destroyed page is usually the free-list head (destroys push on top of
// the chain and undo unwinds in reverse LSN order), but a destroy that is
// still uncommitted can sit above it in the chain.  In that case walk the
// chain and relink the predecessor instead of the head.  Hops latch one page
// at a time and never hold the meta latch across GetPage (the allocator's
// candidate latch is held while it waits for the meta latch -- see
// AllocateNewPage), so a page that an allocator consumed and re-initialized
// mid-walk simply ends the walk; AllocateNewPage's stale-head escape hatch
// then keeps the allocator live and the startup free-list rebuild heals the
// leftover link.
Status PageManager::PopFreePageHead(page_id_t pid, page_id_t next) {
  page_id_t head = 0;
  page_id_t max_page = 0;
  {
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    if (meta->body.meta_page.FirstFreePage() == pid) {
      meta->body.meta_page.PopFreePageHead(pid, next);
      return Status::kSuccess;
    }
    head = meta->body.meta_page.FirstFreePage();
    max_page = meta->body.meta_page.MaxPageCount();
  }
  // A corrupt chain must not spin: bound the walk by the page high-water
  // mark, and retry from a freshly read head in case the list moved under
  // us (an allocator consuming pages above pid makes pid the new head).
  for (int attempt = 0; attempt < 3; ++attempt) {
    page_id_t cursor = head;
    for (page_id_t hops = 0; cursor != 0 && hops <= max_page; ++hops) {
      ASSIGN_OR_RETURN(PageRef, page, pool_->GetPage(cursor, nullptr));
      if (page->Type() != PageType::kFreePage) {
        break;
      }
      if (page->body.free_page.NextFreePage() == pid) {
        page->body.free_page.SetNextFreePage(next);
        return Status::kSuccess;
      }
      cursor = page->body.free_page.NextFreePage();
    }
    if (attempt == 2) {
      break;
    }
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    if (meta->body.meta_page.FirstFreePage() == pid) {
      meta->body.meta_page.PopFreePageHead(pid, next);
      return Status::kSuccess;
    }
    head = meta->body.meta_page.FirstFreePage();
  }
  return Status::kSuccess;
}

StatusOr<PageRef> PageManager::AllocateNewPage(Transaction& system_txn,
                                               PageType new_page_type) {
  // Two-phase allocation: pick the candidate under the meta latch, but
  // release that latch before blocking on the candidate page's latch.  The
  // runtime undo of an aborted destroy latches the candidate page first and
  // takes the meta latch second (LogUndoWithPage -> PopFreePageHead), so
  // holding meta across GetPage here would give the two paths opposite
  // orders and let an allocator deadlock against an aborting destructor.
  // Re-validation under the re-taken latch keeps the pop correct when
  // another allocator moved the head in between; the loser just retries.
  // |force_fresh| skips the free list entirely: set when the free-list
  // head's destroy is still owned by an active transaction, in which
  // case the id cannot be safely reissued (the destroyer's abort would
  // resurrect the old page image and catalog entry over the new owner's
  // content).  The skipped entry becomes allocatable once its destroyer
  // finishes -- a commit leaves the page free with a settled destroyer,
  // while an abort restores it to a non-free type the candidate check
  // rejects (and PopFreePageHead unlinks).
  bool force_fresh = false;
  for (;;) {
    page_id_t candidate = 0;
    {
      ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
      candidate = force_fresh ? meta->body.meta_page.MaxPageCount() + 1
                              : meta->body.meta_page.PeekAllocationCandidate();
    }
    ASSIGN_OR_RETURN(PageRef, page, pool_->GetPage(candidate, nullptr));
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    // Snapshot the allocator before mutating it: the WAL append below can
    // fail (dead logger), and without a rollback the id leaks (max-path ids
    // are skipped forever, free-path ids orphan a free-list entry).  This
    // mirrors MetaPage::DestroyPage's rollback on log-append failure.
    const page_id_t saved_first_free_page =
        meta->body.meta_page.FirstFreePage();
    const page_id_t saved_max_page_count = meta->body.meta_page.MaxPageCount();
    // A candidate popped off the free list is a page id whose previous
    // incarnation was destroyed.  MVCC version chains key rows by
    // {page_id, slot}, so the dead incarnation's chains would otherwise be
    // served to scans of the new page; invalidate them before the id is
    // handed out.  Fresh ids (grown past the high-water mark) carry no
    // chains and skip the shard scan.
    const bool recycled = !force_fresh && saved_first_free_page == candidate;
    if (recycled && page->Type() == PageType::kFreePage) {
      const txn_id_t destroyer = page->body.free_page.Destroyer();
      // A page whose destroyer is still active must not be reissued — not
      // even to the destroying transaction itself.  An abort restores the
      // pre-destroy page image, and the restored rows stay visible only
      // because their version chains survive; reusing the id first would
      // have erased them (see InvalidatePageVersions) and let the restore
      // resurrect invisible rows / clobber the interim owner's content.
      // Once the destroyer settles, reuse is safe: a commit makes the
      // destroy permanent, and an abort unlinks the entry entirely.
      if (destroyer != 0 && system_txn.IsTransactionActive(destroyer)) {
        force_fresh = true;
        continue;
      }
    }
    const bool allocated =
        force_fresh
            ? meta->body.meta_page.AllocateFreshId(candidate)
            : meta->body.meta_page.AllocateCandidate(candidate, page.get());
    if (!allocated) {
      // The candidate moved while the meta latch was released; the pinned
      // page was never mutated, so dropping it and recomputing is safe.
      // A head that still names this candidate while the page is no longer
      // free is a stale link that PopFreePageHead could not relink (its
      // comment covers when that happens); retrying would re-peek the same
      // dead head forever.  Abandon the chain -- the ids return at the
      // startup free-list rebuild -- instead of hanging every allocation.
      if (!force_fresh && meta->body.meta_page.FirstFreePage() == candidate &&
          page->Type() != PageType::kFreePage) {
        meta->body.meta_page.ResetFreeList();
      }
      continue;
    }
    const Status allocate_log =
        system_txn.AllocatePageLog(candidate, new_page_type).GetStatus();
    if (allocate_log != Status::kSuccess) {
      meta->body.meta_page.RollbackAllocation(saved_first_free_page,
                                              saved_max_page_count);
      return allocate_log;
    }
    meta->SetPageLSN(system_txn.PrevRecordEndLSN());
    meta->SetRecLSN(system_txn.PrevRecordEndLSN());
    page->PageInit(candidate, new_page_type);
    if (recycled) {
      system_txn.InvalidatePageVersions(candidate);
    }
    return page;
  }
}

StatusOr<PageRef> PageManager::GetMetaPage() {
  ASSIGN_OR_RETURN(PageRef, meta_page, pool_->GetPage(kMetaPageId, nullptr));
  if (meta_page.IsNull()) {
    return StatusError(StatusCode::kIOError, "failed to get meta page");
  }
  if (meta_page->Type() != PageType::kMetaPage) {
    meta_page->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  return meta_page;
}

}  // namespace tinylamb
