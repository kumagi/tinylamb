#include "page.hpp"

#include <cstring>
#include <iostream>

#include "free_page.hpp"
#include "page/catalog_page.hpp"
#include "page/meta_page.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const PageType& type) {
  switch (type) {
    case PageType::kUnknown:
      o << "UnknownPageType";
      break;
    case PageType::kFreePage:
      o << "FreePageType";
      break;
    case PageType::kMetaPage:
      o << "MetaPageType";
      break;
    case PageType::kCatalogPage:
      o << "CatalogType";
      break;
    case PageType::kRowPage:
      o << "RowType";
      break;
  }
  return o;
}

Page::Page(size_t page_id, PageType type) { PageInit(page_id, type); }

void Page::PageInit(uint64_t pid, PageType page_type) {
  memset(this, 0, kPageSize);
  page_id = pid;
  SetPageLSN(0);
  type = page_type;
  switch (type) {
    case PageType::kUnknown:
      throw std::runtime_error("unknown type won't expect to be PageInit");
    case PageType::kFreePage:
      reinterpret_cast<FreePage*>(this)->Initialize();
      break;
    case PageType::kMetaPage:
      reinterpret_cast<MetaPage*>(this)->Initialize();
      break;
    case PageType::kCatalogPage:
      reinterpret_cast<CatalogPage*>(this)->Initialize();
      break;
    case PageType::kRowPage:
      reinterpret_cast<RowPage*>(this)->Initialize();
      break;
  }
}

void Page::SetChecksum() const { checksum = std::hash<Page>()(*this); }

void Page::InsertImpl(const RowPosition& pos, std::string_view redo) {
  switch (Type()) {
    case PageType::kRowPage: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->InsertRow(redo);
      break;
    }
    case PageType::kCatalogPage: {
      auto* cp = reinterpret_cast<CatalogPage*>(this);
      cp->InsertSchema(redo);
      break;
    }
    default: {
      LOG(ERROR) << "Cannot apply Insert log";
      break;
    }
  }
}

void Page::UpdateImpl(const RowPosition& pos, std::string_view redo) {
  switch (Type()) {
    case PageType::kRowPage: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->UpdateRow(pos.slot, redo);
      break;
    }
    case PageType::kCatalogPage: {
      auto* cp = reinterpret_cast<CatalogPage*>(this);
      cp->UpdateSchema(pos, redo);
      break;
    }
    default:
      LOG(ERROR) << "Cannot apply Insert log";
      break;
  }
}

void Page::DeleteImpl(const RowPosition& pos) {
  switch (Type()) {
    case PageType::kRowPage: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->DeleteRow(pos.slot);
      break;
    }
    case PageType::kCatalogPage: {
      auto* cp = reinterpret_cast<CatalogPage*>(this);
      cp->DeleteSchema(pos);
      break;
    }
    default:
      LOG(ERROR) << "Cannot apply Insert log";
      break;
  }
}

bool Page::IsValid() const {
  switch (type) {
    case tinylamb::PageType::kUnknown:
      return false;
    case PageType::kMetaPage: {
      return reinterpret_cast<const MetaPage*>(this)->IsValid();
    }
    case PageType::kCatalogPage: {
      return reinterpret_cast<const CatalogPage*>(this)->IsValid();
    }
    case PageType::kRowPage: {
      return reinterpret_cast<const RowPage*>(this)->IsValid();
    }
    case PageType::kFreePage:
      return reinterpret_cast<const FreePage*>(this)->IsValid();
  }
  return false;
}

void* Page::operator new(size_t page_id) {
  void* ret = new char[kPageSize];
  memset(ret, 0, kPageSize);
  return ret;
}

void Page::operator delete(void* page) noexcept {
  delete[] reinterpret_cast<char*>(page);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Page>::operator()(const tinylamb::Page& p) const {
  uint64_t header_hash =
      std::hash<uint64_t>()(p.page_id) + std::hash<uint64_t>()(p.PageLSN()) +
      std::hash<uint64_t>()(static_cast<unsigned long>(p.type));
  switch (p.type) {
    case tinylamb::PageType::kUnknown:
      throw std::runtime_error("Do not hash against unknown page");
    case tinylamb::PageType::kFreePage:
      return header_hash + std::hash<tinylamb::FreePage>()(
                               reinterpret_cast<const tinylamb::FreePage&>(p));
    case tinylamb::PageType::kMetaPage:
      return header_hash + std::hash<tinylamb::MetaPage>()(
                               reinterpret_cast<const tinylamb::MetaPage&>(p));
    case tinylamb::PageType::kCatalogPage:
      return header_hash +
             std::hash<tinylamb::CatalogPage>()(
                 reinterpret_cast<const tinylamb::CatalogPage&>(p));
    case tinylamb::PageType::kRowPage:
      return header_hash + std::hash<tinylamb::RowPage>()(
                               reinterpret_cast<const tinylamb::RowPage&>(p));
  }
  throw std::runtime_error("unknown page type");
}
