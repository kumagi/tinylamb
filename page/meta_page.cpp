#include "page/meta_page.hpp"

#include <memory>
#include <sstream>

#include "page/free_page.hpp"
#include "page/page_pool.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

PageRef MetaPage::AllocateNewPage(Transaction& txn, PagePool& pool,
                                  PageType new_page_type) {
  uint64_t new_page_id;
  PageRef ret = [&]() {
    if (first_free_page == 0) {
      new_page_id = ++max_page_count;
      return pool.GetPage(max_page_count);
    } else {
      new_page_id = first_free_page;
      PageRef page = pool.GetPage(new_page_id);
      first_free_page = page.GetFreePage().next_free_page;
      return page;
    }
  }();
  ret->PageInit(new_page_id, new_page_type);
  txn.AllocatePageLog(new_page_id, new_page_type);
  return ret;
}

// Precondition: latch of page is taken by txn.
void MetaPage::DestroyPage(Transaction& txn, Page* target, PagePool& pool) {
  uint64_t free_page_id = target->PageId();
  target->PageInit(free_page_id, PageType::kFreePage);
  assert(target->PageId() == free_page_id);
  FreePage& free_page = target->body.free_page;
  LOG(INFO) << "target: " << target->PageId() << " expect: " << free_page_id;
  assert(target->PageId() == free_page_id);
  // Add the free page to the free page chain.
  free_page.next_free_page = first_free_page;
  assert(target->PageId() == free_page_id);
  first_free_page = free_page_id;
  txn.DestroyPageLog(free_page_id);
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::MetaPage>::operator()(
    const tinylamb::MetaPage& m) {
  const uint64_t kChecksumSalt = 0xbe1a0a4;
  return kChecksumSalt + std::hash<uint64_t>()(m.max_page_count) +
         std::hash<uint64_t>()(m.first_free_page);
}
