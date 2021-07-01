#include "page_manager.hpp"

#include <recovery/log_record.hpp>
#include <sstream>

#include "recovery/logger.hpp"

namespace tinylamb {

PageManager::PageManager(std::string_view name, size_t capacity)
    : pool_(name, capacity) {
  GetMetaPage();
  UnpinMetaPage();
}

Page* PageManager::GetPage(uint64_t page_id) {
  MetaPage& m = GetMetaPage();
  m.MetaData().max_page_count = std::max(m.MetaData().max_page_count, page_id);
  UnpinMetaPage();
  return pool_.GetPage(page_id);
}

// Logically delete the page.
void PageManager::DestroyPage(Transaction& system_txn, Page* target) {
  MetaPage& m = GetMetaPage();
  m.DestroyPage(system_txn, target, pool_);
  UnpinMetaPage();
}

Page* PageManager::AllocateNewPage(Transaction& system_txn, PageType type) {
  MetaPage& m = GetMetaPage();
  Page* new_page = m.AllocateNewPage(system_txn, type, pool_);
  UnpinMetaPage();
  return new_page;
}

MetaPage& PageManager::GetMetaPage() {
  Page* meta_page = pool_.GetPage(kMetaPageId);
  if (meta_page == nullptr) {
    throw std::runtime_error("failed to get meta page");
  }
  if (meta_page->Type() != PageType::kMetaPage) {
    LOG_ERROR("Unrecoverable meta page crash detected, initialize all");
    meta_page->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  return *reinterpret_cast<MetaPage*>(meta_page);
}

}  // namespace tinylamb