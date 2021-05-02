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

namespace pedasus {

enum class PageType : uint64_t {
  kUnknown = 0,
  kFixedRow,
  kVariableRow,
};

class Page {
 public:
  struct PageHeader {
    uint64_t page_id = 0;
    uint64_t last_lsn = 0;
    enum PageType type = PageType::kUnknown;
  };

  Page(std::fstream& src, size_t page_id);
  void WriteBack(std::fstream& file_desc);
  uint8_t* Payload() {return buffer_; }
  size_t PayloadSize() const { return kPageSize - sizeof(PageHeader); }

 public:
  PageHeader header_;
  uint8_t buffer_[kPageSize - sizeof(PageHeader)];
};

class FixedRowPage {
  Row* GetRowPtr(size_t index) {
    return nullptr;
  }
};

class VariableRowPage {
 public:
};

}  // namespace pedasus
#endif // PEDASOS_PAGE_HPP
