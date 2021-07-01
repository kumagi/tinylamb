#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <unistd.h>

#include <iostream>

#include "constants.hpp"
#include "macro.hpp"

namespace tinylamb {

enum class PageType : uint64_t {
  kUnknown = 0,
  kFreePage,
  kMetaPage,
  kCatalogPage,
  kFixedLengthRow,
  kVariableRow,
};

std::ostream& operator<<(std::ostream& o, const PageType& type);

struct PageHeader {
  uint64_t page_id = 0;
  uint64_t last_lsn = 0;
  enum PageType type = PageType::kUnknown;
  [[nodiscard]] uint64_t CalcChecksum() const;
  mutable uint64_t checksum = 0;
  void Initialize(uint64_t pid, PageType t) {
    page_id = pid;
    last_lsn = 0;
    type = t;
  }
};

struct Page {
 public:
  const static size_t kBodySize = kPageSize - sizeof(PageHeader);
  Page(size_t page_id, PageType type);
  void PageInit(uint64_t page_id, PageType type);

  PageHeader& Header() { return *reinterpret_cast<PageHeader*>(payload); }
  [[nodiscard]] const PageHeader& Header() const {
    return *reinterpret_cast<const PageHeader*>(payload);
  }
  [[nodiscard]] uint64_t PageId() const { return Header().page_id; }
  [[nodiscard]] PageType Type() const { return Header().type; }
  template <typename T>
  T& BodyAs() {
    return *reinterpret_cast<T*>(payload + sizeof(PageHeader));
  }
  template <typename T>
  const T& BodyAs() const {
    return *reinterpret_cast<const T*>(payload + sizeof(PageHeader));
  }
  char* Body() { return reinterpret_cast<char*>(payload + sizeof(PageHeader)); }
  [[nodiscard]] const char* Body() const {
    return reinterpret_cast<const char*>(payload + sizeof(PageHeader));
  }
  char* Data() { return reinterpret_cast<char*>(payload); }
  [[nodiscard]] const char* Data() const {
    return reinterpret_cast<const char*>(payload);
  }
  void SetChecksum() const;
  [[nodiscard]] uint64_t GetChecksum() const;
  [[nodiscard]] bool IsValid() const;
  char payload[kPageSize];
};

static_assert(sizeof(Page) == kPageSize);

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_HPP
