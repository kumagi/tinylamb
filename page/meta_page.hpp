#ifndef TINYLAMB_META_PAGE_HPP
#define TINYLAMB_META_PAGE_HPP

#include <cstdint>

#include "page/page.hpp"

namespace tinylamb {

class Transaction;
class PagePool;

struct MetaPageHeader {
  MAPPING_ONLY(MetaPageHeader);
  uint64_t max_page_count;
  uint64_t first_free_page;
  void Initialize() {
    max_page_count = 0;
    first_free_page = 0;
  }
  [[nodiscard]] uint64_t CalcChecksum() const {
    return std::hash<uint64_t>()(max_page_count) +
           std::hash<uint64_t>()(first_free_page);
  }
};

struct MetaPage : public Page {
  void Initialize() { BodyAs<MetaPageHeader>().Initialize(); }
  [[nodiscard]] uint64_t CalcChecksum() const {
    const uint64_t kChecksumSalt = 0xbe1a0a4;
    uint64_t result = kChecksumSalt;
    result += Header().CalcChecksum();
    result += BodyAs<MetaPageHeader>().CalcChecksum();
    return result;
  }
  MetaPageHeader& MetaData() { return BodyAs<MetaPageHeader>(); }
  Page* AllocateNewPage(Transaction& txn, PageType type, PagePool& pool);
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);
};
static_assert(sizeof(MetaPage) == sizeof(Page),
              "no member allowed in page mapping class");

}  // namespace tinylamb

#endif  // TINYLAMB_META_PAGE_HPP
