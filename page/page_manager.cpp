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
#include <stdexcept>
#include <string_view>

#include "page/meta_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

PageManager::PageManager(std::string_view db_name, size_t capacity)
    : pool_(db_name, capacity) {
  GetMetaPage();
}

PageRef PageManager::GetPage(uint64_t page_id, bool shared) {
  bool cache_hit = false;
  PageRef ref = pool_.GetPage(page_id, &cache_hit, shared);
  if (!cache_hit && !ref->IsValid()) {
    // Found a broken or new page.
    return {};
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
void PageManager::DestroyPage(Transaction& system_txn, Page* target) {
  GetMetaPage()->DestroyPage(system_txn, target);
}

// D3 (docs/design.md): undo restored the destroyed page; drop it from the
// allocator free stack so the next AllocateNewPage cannot re-issue it.
void PageManager::PopFreePageHead(page_id_t pid, page_id_t next) {
  PageRef meta = GetMetaPage();
  meta->body.meta_page.PopFreePageHead(pid, next);
}

PageRef PageManager::AllocateNewPage(Transaction& system_txn,
                                     PageType new_page_type) {
  return GetMetaPage()->AllocateNewPage(system_txn, pool_, new_page_type);
}

PageRef PageManager::GetMetaPage() {
  PageRef meta_page = pool_.GetPage(kMetaPageId, nullptr);
  if (meta_page.IsNull()) {
    throw std::runtime_error("failed to get meta page");
  }
  if (meta_page->Type() != PageType::kMetaPage) {
    meta_page->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  return meta_page;
}

}  // namespace tinylamb
