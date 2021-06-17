#ifndef TINYLAMB_CATALOG_HPP
#define TINYLAMB_CATALOG_HPP

#include <cstdint>
#include <string>

#include "macro.hpp"
#include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Catalog {
  static constexpr uint64_t kCatalogPageId = 1;
  CatalogPage* GetCatalogPage() {
    return reinterpret_cast<CatalogPage*>(pm_->GetPage(kCatalogPageId));
  }
 public:
  explicit Catalog(PageManager* pm)
      : pm_(pm) {
    auto* catalog_page = GetCatalogPage();
    if (!catalog_page->IsValid()) {
      catalog_page->Initialize();
    }
    pm_->Unpin(catalog_page->PageId());
  }
  void Initialize() {
    auto* catalog_page = GetCatalogPage();
    catalog_page->Initialize();
    pm_->Unpin(catalog_page->PageId());
  }

  // Return false if there is the same name table is already exists.
  bool CreateTable(Transaction& txn, const Schema& schema) {
    auto* catalog_page = GetCatalogPage();
    Page* data_page = pm_->AllocateNewPage();
    RowPosition result = catalog_page->AddSchema(txn, schema);
    pm_->Unpin(data_page->Header().page_id);
    pm_->Unpin(catalog_page->PageId());
    return result.IsValid();
  }

  Schema GetSchema(std::string_view table_name) {
    uint64_t page_id = kCatalogPageId;
    while (page_id != 0) {
      CatalogPage* catalog_page =
          reinterpret_cast<CatalogPage*>(pm_->GetPage(page_id));
      for (size_t i = 0; i < catalog_page->SlotCount(); ++i) {
        RowPosition pos(page_id, i);
        const Schema schema = catalog_page->Read(pos);
        if (schema.Name() == table_name) {
          return schema;
        } else {
          std::cerr << "no match: " << schema << "\n";
        }
      }
      page_id = catalog_page->NextPageID();
      pm_->Unpin(catalog_page->PageId());
    }
    throw std::runtime_error("Table not found: " + std::string(table_name));
  }

  [[nodiscard]] size_t Schemas() const {
    size_t ret = 0;
    uint64_t page_id = kCatalogPageId;
    while (page_id != 0) {
      const CatalogPage* catalog_page =
          reinterpret_cast<CatalogPage*>(pm_->GetPage(page_id));
      ret += catalog_page->SlotCount();
      page_id = catalog_page->NextPageID();
      pm_->Unpin(catalog_page->PageId());
    }
    return ret;
  }

  [[maybe_unused]] void DebugDump(std::ostream& o) {
    uint64_t page_id = kCatalogPageId;
    while (page_id != 0) {
      const CatalogPage* catalog_page =
          reinterpret_cast<CatalogPage*>(pm_->GetPage(page_id));
      o << *catalog_page;
      page_id = catalog_page->NextPageID();
      pm_->Unpin(catalog_page->PageId());
    }
    auto* page = GetCatalogPage();
  }

 private:
  PageManager* pm_;
};

} // namespace tinylamb


#endif  // TINYLAMB_CATALOG_HPP
