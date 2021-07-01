#include "page/meta_page.hpp"

#include <memory>
#include <sstream>

#include "page/free_page.hpp"
#include "page/page_pool.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

Page* MetaPage::AllocateNewPage(Transaction& txn, PageType type,
                                PagePool& pool) {
  MetaPageHeader& meta_data = MetaData();
  Page* ret;
  uint64_t new_page_id;
  if (meta_data.first_free_page == 0) {
    new_page_id = ++meta_data.max_page_count;
    ret = pool.GetPage(meta_data.max_page_count);
  } else {
    new_page_id = meta_data.first_free_page;
    ret = pool.GetPage(new_page_id);

    meta_data.first_free_page =
        reinterpret_cast<FreePageHeader*>(ret)->next_free_page;
  }
  ret->PageInit(new_page_id, type);
  txn.AllocatePageLog(new_page_id);
  txn.PreCommit();  // No need to wait for the log to be durable.
  Header().last_lsn = txn.PrevLSN();
  return ret;
}

// Precondition: latch of page is taken by txn.
void MetaPage::DestroyPage(Transaction& txn, Page* target, PagePool& pool) {
  uint64_t free_page_id = target->PageId();
  MetaPageHeader& meta_data = MetaData();
  target->PageInit(free_page_id, PageType::kFreePage);
  assert(target->PageId() == free_page_id);
  auto* free_page = reinterpret_cast<FreePage*>(target);
  assert(target->PageId() == free_page_id);
  // Add the free page to the free page chain.
  free_page->MetaData().next_free_page = meta_data.first_free_page;
  assert(target->PageId() == free_page_id);
  meta_data.first_free_page = free_page_id;
  txn.DestroyPageLog(free_page_id);
  txn.PreCommit();
  Header().last_lsn = txn.PrevLSN();
}

}  // namespace tinylamb