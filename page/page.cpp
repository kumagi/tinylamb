#include "page.hpp"

#include <unistd.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

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

[[nodiscard]] uint64_t PageHeader::CalcChecksum() const {
  return std::hash<uint64_t>()(page_id) + std::hash<uint64_t>()(last_lsn) +
         std::hash<uint64_t>()(static_cast<unsigned long>(type));
}

Page::Page(size_t page_id, PageType type) { PageInit(page_id, type); }

void Page::PageInit(uint64_t page_id, PageType type) {
  memset(payload, 0, kPageSize);
  Header().Initialize(page_id, type);
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
      throw std::runtime_error("Initialization of VariableRow is not implemented");
  }
}

void Page::SetChecksum() const {
  uint64_t checksum = GetChecksum();
  Header().checksum = checksum;
}

uint64_t Page::GetChecksum() const {
  constexpr uint64_t kChecksumSalt = 0xcafebabe;
  uint64_t checksum = kChecksumSalt + Header().CalcChecksum();
  switch (Type()) {
    case PageType::kUnknown:
      throw std::runtime_error("unknown type page " + std::to_string(PageId()) +
                               " cannot calculate the checksum");
    case PageType::kMetaPage: {
      checksum += reinterpret_cast<const MetaPage*>(this)->CalcChecksum();
      break;
    }
    case PageType::kCatalogPage: {
      checksum += reinterpret_cast<const CatalogPage*>(this)->CalcChecksum();
      break;
    }
    case PageType::kFixedLengthRow: {
      checksum += reinterpret_cast<const RowPage*>(this)->CalcChecksum();
      break;
    }
    case PageType::kVariableRow:
      break;
  }
  Header().checksum = checksum;
  return checksum;
}

bool Page::IsValid() const { return Header().checksum == GetChecksum(); }

}  // namespace tinylamb