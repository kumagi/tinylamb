#include "page_manager.hpp"

#include <sstream>

#include "page/meta_page.hpp"
#include "page/page_ref.hpp"
#include "recovery/log_record.hpp"

namespace tinylamb {

PageManager::PageManager(std::string_view name, size_t capacity)
    : pool_(name, capacity) {
  GetMetaPage();
}

PageRef PageManager::GetPage(uint64_t page_id) {
  PageRef page(GetMetaPage());
  MetaPage* m = page.AsMetaPage();
  m->max_page_count = std::max(m->max_page_count, page_id);
  return pool_.GetPage(page_id);
}

// Logically delete the page.
void PageManager::DestroyPage(Transaction& system_txn, Page* target) {
  GetMetaPage().AsMetaPage()->DestroyPage(system_txn, target, pool_);
}

PageRef PageManager::AllocateNewPage(Transaction& system_txn,
                                     PageType new_page_type) {
  return GetMetaPage().AsMetaPage()->AllocateNewPage(system_txn, pool_,
                                                     new_page_type);
}

PageRef PageManager::GetMetaPage() {
  PageRef meta_page = pool_.GetPage(kMetaPageId);
  if (meta_page.IsNull()) {
    throw std::runtime_error("failed to get meta page");
  }
  if (meta_page->Type() != PageType::kMetaPage) {
    meta_page->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  return std::move(meta_page);
}

}  // namespace tinylamb