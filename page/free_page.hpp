#ifndef TINYLAMB_FREE_PAGE_HPP
#define TINYLAMB_FREE_PAGE_HPP

#include "macro.hpp"
#include "page/page.hpp"

namespace tinylamb {

struct FreePageHeader {
  MAPPING_ONLY(FreePageHeader);
  uint64_t next_free_page;
  void Initialize() { next_free_page = 0; }
};

struct FreePage : public Page {
  MAPPING_ONLY(FreePage);
  static constexpr size_t kFreeBodySize =
      Page::kBodySize - sizeof(FreePageHeader);
  void Initialize() { MetaData().Initialize(); }
  char* FreeBody() { return Body() + sizeof(FreePageHeader); }
  FreePageHeader& MetaData() { return BodyAs<FreePageHeader>(); }
};

}  // namespace tinylamb

#endif  // TINYLAMB_FREE_PAGE_HPP
