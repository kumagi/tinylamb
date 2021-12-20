#include "type/catalog.hpp"

#include <cstdint>
#include <string>

#include "log_message.hpp"
#include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

CatalogPage* Catalog::GetCatalogPage() {
  return pm_->GetPage(kCatalogPageId).AsCatalogPage();
}

Catalog::Catalog(PageManager* pm) : pm_(pm) {
  auto* catalog_page = GetCatalogPage();
  if (catalog_page->Type() != PageType::kCatalogPage) {
    LOG(ERROR) << "No catalog found, initializing";
    catalog_page->PageInit(kCatalogPageId, PageType::kCatalogPage);
    catalog_page->Initialize();
  }
}

void Catalog::Initialize() {
  auto* catalog_page = GetCatalogPage();
  catalog_page->Initialize();
}

bool Catalog::CreateTable(Transaction& txn, Schema& schema) {
  auto* catalog_page = GetCatalogPage();
  Transaction new_page_txn = txn.SpawnSystemTransaction();
  RowPosition result = catalog_page->AddSchema(txn, schema);

  // TODO: fix if schema may have variable length data.
  return result.IsValid();
}

Schema Catalog::GetSchema(Transaction& txn, std::string_view table_name) {
  uint64_t page_id = kCatalogPageId;
  while (page_id != 0) {
    CatalogPage* catalog_page = pm_->GetPage(page_id).AsCatalogPage();
    for (size_t i = 0; i < catalog_page->SlotCount(); ++i) {
      RowPosition pos(page_id, i);
      Schema schema = catalog_page->Read(txn, pos);
      if (schema.Name() == table_name) {
        return schema;
      }
    }
    page_id = catalog_page->NextPageID();
  }
  throw std::runtime_error("Table not found: " + std::string(table_name));
}

[[nodiscard]] size_t Catalog::Schemas() const {
  size_t ret = 0;
  uint64_t page_id = kCatalogPageId;
  while (page_id != 0) {
    const CatalogPage* catalog_page = pm_->GetPage(page_id).AsCatalogPage();
    ret += catalog_page->SlotCount();
    page_id = catalog_page->NextPageID();
  }
  return ret;
}

[[maybe_unused]] void Catalog::DebugDump(std::ostream& o) {
  uint64_t page_id = kCatalogPageId;
  while (page_id != 0) {
    const CatalogPage* catalog_page = pm_->GetPage(page_id).AsCatalogPage();
    o << *catalog_page;
    page_id = catalog_page->NextPageID();
  }
  auto* page = GetCatalogPage();
}

}  // namespace tinylamb
