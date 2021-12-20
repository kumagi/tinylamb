#ifndef TINYLAMB_META_PAGE_HPP
#define TINYLAMB_META_PAGE_HPP

#include <cstdint>

#include "page/page.hpp"
#include "page/page_ref.hpp"

namespace tinylamb {

class Transaction;
class PagePool;

struct MetaPage : public Page {
  void Initialize() {
    max_page_count = 0;
    first_free_page = 0;
  }

  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                        PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target, PagePool& pool);

  uint64_t max_page_count;
  uint64_t first_free_page;
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
