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
void PageManager::PopFreePageHead(page_id_t pid, page_id_t next) {
  PageRef meta = GetMetaPage().MoveValue();
  meta->body.meta_page.PopFreePageHead(pid, next);
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
  for (;;) {
    page_id_t candidate = 0;
    {
      ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
      candidate = meta->body.meta_page.PeekAllocationCandidate();
    }
    ASSIGN_OR_RETURN(PageRef, page, pool_->GetPage(candidate, nullptr));
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    if (!meta->body.meta_page.AllocateCandidate(candidate, page.get())) {
      // The candidate moved while the meta latch was released; the pinned
      // page was never mutated, so dropping it and recomputing is safe.
      continue;
    }
    RETURN_IF_FAIL(
        system_txn.AllocatePageLog(candidate, new_page_type).GetStatus());
    meta->SetPageLSN(system_txn.PrevRecordEndLSN());
    meta->SetRecLSN(system_txn.PrevRecordEndLSN());
    page->PageInit(candidate, new_page_type);
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
