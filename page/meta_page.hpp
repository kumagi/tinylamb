#ifndef TINYLAMB_META_PAGE_HPP
#define TINYLAMB_META_PAGE_HPP

#include <cstdint>

#include "page/page_ref.hpp"
#include "page/page_type.hpp"

namespace tinylamb {

class Transaction;
class Page;
class PagePool;

class MetaPage {
  void Initialize() {
    max_page_count = 0;
    first_free_page = 0;
  }

  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                          PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);

  // Note that all member of this class is private.
  // Only Page class can access these members.
  friend class Page;
  friend std::hash<tinylamb::MetaPage>;

  uint64_t max_page_count;
  uint64_t first_free_page;
  void Dump(std::ostream& o, int) const;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::MetaPage> {
 public:
  uint64_t operator()(const tinylamb::MetaPage& m);
};
}  // namespace std

#endif  // TINYLAMB_META_PAGE_HPP
