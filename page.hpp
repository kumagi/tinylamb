#ifndef PEDASOS_PAGE_HPP
#define PEDASOS_PAGE_HPP

#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>

#include "Row.hpp"
#include "constants.hpp"
#include "macro.hpp"

namespace tinylamb {

enum class PageType : uint64_t {
  kUnknown = 0,
  kMetaPage,
  kCatalogPage,
  kFixedRow,
  kVariableRow,
};

class Page {
 public:
  struct Header {
    uint64_t page_id = 0;
    uint64_t last_lsn = 0;
    enum PageType type = PageType::kUnknown;
  };

  Page(std::fstream& src, size_t page_id);
  void WriteBack(std::fstream& file_desc);
  [[nodiscard]] static constexpr size_t PayloadSize() {
    return kPageSize - sizeof(Header);
  }

 public:
  Header header;
  uint8_t payload[kPageSize - sizeof(Header)];
};

static_assert(sizeof(Page) == kPageSize);

}  // namespace tinylamb

#endif // PEDASOS_PAGE_HPP
