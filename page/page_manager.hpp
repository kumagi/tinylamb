#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include "../macro.hpp"
#include "page.hpp"
#include "page_pool.hpp"

namespace tinylamb {

class PageManager {
  static constexpr uint64_t kManagerMetaPageId = 0;
  static constexpr uint64_t kCatalogPageId = 1;
  struct FreePageNode {
    MAPPING_ONLY(FreePageNode);
    uint64_t next_free_page;
    static FreePageNode* AsMetaPageNode(Page* page) {
      return reinterpret_cast<FreePageNode*>(page->payload);
    }
  };

  struct MetaPage {
    MAPPING_ONLY(MetaPage);
    uint64_t max_page_count;
    uint64_t first_free_page;
    uint64_t last_free_page;
    static MetaPage* AsMetaPage(Page* page) {
      return reinterpret_cast<MetaPage*>(page->payload);
    }
    void Initialize() {
      max_page_count = 0;
      first_free_page = 0;
      last_free_page = 0;
    }
  };

 public:
  PageManager(std::string_view name, size_t capacity) : pool_(name, capacity) {
    Page* page = pool_.GetPage(kManagerMetaPageId);
    Page::Header* header = &page->header;
    if (header->type != PageType::kMetaPage) {
      // Initialize entire managed pages.
      header->type = PageType::kMetaPage;
      MetaPage* meta_page = MetaPage::AsMetaPage(page);
      meta_page->Initialize();
    }
    pool_.Unpin(kManagerMetaPageId);
  }

  Page* GetPage(int64_t page_id) {
    return pool_.GetPage(page_id);
  }

  Page* GetCatalogPage() {
    Page* catalog_page = pool_.GetPage(kCatalogPageId);
    catalog_page->header.type = PageType::kCatalogPage;
    return pool_.GetPage(kCatalogPageId);
  }

  void UnpinCatalogPage() {
    pool_.Unpin(kCatalogPageId);
  }

  void DestroyPage(Page* target) {
    MetaPage* m = GetMetaPage();
    auto* free_page = FreePageNode::AsMetaPageNode(target);
    uint64_t free_page_id = target->header.page_id;
    pool_.Unpin(free_page_id);
    if (m->last_free_page == 0) {
      // This is the first free page.
      m->last_free_page = m->first_free_page = free_page_id;
    } else {
      // Add the chain to free page list.
      Page* last_page = pool_.GetPage(m->last_free_page);
      auto* tail = FreePageNode::AsMetaPageNode(last_page);
      tail->next_free_page = free_page_id;
      m->last_free_page = free_page_id;
      pool_.Unpin(last_page->header.page_id);
    }
    UnpinMetaPage();
  }

  Page* AllocateNewPage() {
    MetaPage* m = GetMetaPage();
    if (m->last_free_page == 0) {
      m->max_page_count += 1;
      UnpinMetaPage();
      return pool_.GetPage(m->max_page_count);
    } else {
      Page* new_page = pool_.GetPage(m->first_free_page);
      auto* new_free_page = FreePageNode::AsMetaPageNode(new_page);
      m->first_free_page = new_free_page->next_free_page;
      if (m->first_free_page == 0) {
        m->last_free_page = 0;
      }
      UnpinMetaPage();
      return new_page;
    }
  }

  bool Unpin(size_t page_id) {
    return pool_.Unpin(page_id);
  }

 private:
  MetaPage* GetMetaPage() {
    Page* meta_page = pool_.GetPage(kManagerMetaPageId);
    assert(meta_page->header.type == PageType::kMetaPage);
    return MetaPage::AsMetaPage(meta_page);
  }

  void UnpinMetaPage() {
    pool_.Unpin(kManagerMetaPageId);
  }

  PagePool pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP

