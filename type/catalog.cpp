#include "type/catalog.hpp"

#include <cstdint>
#include <string>

#include "log_message.hpp"
#include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

CatalogPage* Catalog::GetCatalogPage() {
  return reinterpret_cast<CatalogPage*>(pm_->GetPage(kCatalogPageId));
}

Catalog::Catalog(PageManager* pm) : pm_(pm) {
  auto* catalog_page = GetCatalogPage();
  if (catalog_page->Type() != PageType::kCatalogPage) {
    LOG(ERROR) << "No catalog found, initializing";
    catalog_page->PageInit(kCatalogPageId, PageType::kCatalogPage);
    catalog_page->Initialize();
  }
  pm_->Unpin(catalog_page->PageId());
}

void Catalog::Initialize() {
  auto* catalog_page = GetCatalogPage();
  catalog_page->Initialize();
  pm_->Unpin(catalog_page->PageId());
}

bool Catalog::CreateTable(Transaction& txn, Schema& schema) {
  auto* catalog_page = GetCatalogPage();
  Transaction new_page_txn = txn.SpawnSystemTransaction();
  auto* data_page = reinterpret_cast<RowPage*>(
      pm_->AllocateNewPage(new_page_txn, PageType::kFixedLengthRow));
  RowPosition result = catalog_page->AddSchema(txn, schema);
  data_page->row_size_ = schema.FixedRowSize();

  // TODO: fix if schema may have variable length data.
  auto* rp = reinterpret_cast<RowPage*>(data_page);
  rp->row_size_ = schema.FixedRowSize();
  LOG(DEBUG) << this << "row size: " << rp->row_size_ << " of " << rp->PageId();

  pm_->Unpin(data_page->page_id);
  pm_->Unpin(catalog_page->PageId());
  return result.IsValid();
}

Schema Catalog::GetSchema(Transaction& txn, std::string_view table_name) {
  uint64_t page_id = kCatalogPageId;
  while (page_id != 0) {
    auto* catalog_page = reinterpret_cast<CatalogPage*>(pm_->GetPage(page_id));
    for (size_t i = 0; i < catalog_page->SlotCount(); ++i) {
      RowPosition pos(page_id, i);
      Schema schema = catalog_page->Read(txn, pos);
      if (schema.Name() == table_name) {
        pm_->Unpin(catalog_page->PageId());
        return schema;
      }
    }
    page_id = catalog_page->NextPageID();
    pm_->Unpin(catalog_page->PageId());
  }
  throw std::runtime_error("Table not found: " + std::string(table_name));
}

[[nodiscard]] size_t Catalog::Schemas() const {
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

[[maybe_unused]] void Catalog::DebugDump(std::ostream& o) {
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

}  // namespace tinylamb
