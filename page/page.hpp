#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>

#include "../constants.hpp"
#include "../macro.hpp"
#include "../row.hpp"

namespace tinylamb {

enum class PageType : uint64_t {
  kUnknown = 0,
  kMetaPage,
  kCatalogPage,
  kFixedLengthRow,
  kVariableRow,
};

struct PageHeader {
  uint64_t page_id = 0;
  uint64_t last_lsn = 0;
  enum PageType type = PageType::kUnknown;
};

struct Page {
 public:
  Page(std::fstream& src, size_t page_id);
  PageHeader& Header() {
    return *reinterpret_cast<PageHeader*>(payload);
  }
  [[nodiscard]] const PageHeader& Header() const {
    return *reinterpret_cast<const PageHeader*>(payload);
  }
  uint64_t PageId() const {
    return Header().page_id;
  }
  template <typename Type>
  Type& Body() {
    return *reinterpret_cast<Type*>(payload + sizeof(PageHeader));
  }
  template <typename Type>
  const Type& Body() const {
    return *reinterpret_cast<const Type*>(payload + sizeof(PageHeader));
  }
  char* Data() {
    return reinterpret_cast<char*>(payload);
  }
  [[nodiscard]] const char* Data() const {
    return reinterpret_cast<const char*>(payload);
  }

  uint8_t payload[kPageSize];
};

static_assert(sizeof(Page) == kPageSize);

}  // namespace tinylamb

#endif // TINYLAMB_PAGE_HPP
