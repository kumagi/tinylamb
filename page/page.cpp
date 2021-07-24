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
    case PageType::kFixedLengthRow:
      o << "FixedLengthRowType";
      break;
    case PageType::kVariableRow:
      o << "VariableRowType";
      break;
  }
  return o;
}

Page::Page(size_t page_id, PageType type) { PageInit(page_id, type); }

void Page::PageInit(uint64_t pid, PageType page_type) {
  memset(this, 0, kPageSize);
  page_id = pid;
  last_lsn = 0;
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
    case PageType::kFixedLengthRow:
      reinterpret_cast<RowPage*>(this)->Initialize();
      break;
    case PageType::kVariableRow:
      throw std::runtime_error(
          "Initialization of VariableRow is not implemented");
  }
}

void Page::SetChecksum() const { checksum = std::hash<Page>()(*this); }

void Page::InsertImpl(std::string_view redo) {
  switch (Type()) {
    case PageType::kFixedLengthRow: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->InsertRow(redo);
      break;
    }
    case PageType::kVariableRow: {
      throw std::runtime_error("Not implemented error");
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
    case PageType::kFixedLengthRow: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->UpdateRow(pos, redo);
      break;
    }
    case PageType::kVariableRow: {
      throw std::runtime_error("Not implemented error");
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
    case PageType::kFixedLengthRow: {
      auto* rp = reinterpret_cast<RowPage*>(this);
      rp->DeleteRow(pos);
      break;
    }
    case PageType::kVariableRow: {
      throw std::runtime_error("Not implemented error");
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

bool Page::IsValid() const { return checksum == std::hash<Page>()(*this); }

void* Page::operator new(size_t page_id) {
  return new char[kPageSize];
}

void Page::operator delete(void* page) noexcept {
  delete[] reinterpret_cast<char*>(page);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Page>::operator()(const tinylamb::Page& p) const {
  uint64_t header_hash =
      std::hash<uint64_t>()(p.page_id) + std::hash<uint64_t>()(p.last_lsn) +
      std::hash<uint64_t>()(static_cast<unsigned long>(p.type));
  switch (p.type) {
    case tinylamb::PageType::kUnknown:
      throw std::runtime_error("Do not hash agains unknown page");
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
    case tinylamb::PageType::kFixedLengthRow:
      return header_hash + std::hash<tinylamb::RowPage>()(
                               reinterpret_cast<const tinylamb::RowPage&>(p));
    case tinylamb::PageType::kVariableRow:
      throw std::runtime_error("not implemented");
  }
  throw std::runtime_error("unknown page type");
}
