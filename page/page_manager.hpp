#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include <string_view>

#include "log_message.hpp"
#include "page/page.hpp"
#include "page/page_pool.hpp"
#include "recovery/recovery.hpp"

namespace tinylamb {

class Logger;
class Recovery;
class MetaPage;
class Transaction;
class TransactionManager;

class PageManager {
  static constexpr uint64_t kMetaPageId = 0;

 public:
  PageManager(std::string_view db_name, size_t capacity);

  PageRef GetPage(uint64_t page_id);

  PageRef AllocateNewPage(Transaction& txn, PageType new_page_type);

  // Logically delete the page.
  void DestroyPage(Transaction& txn, Page* target);

  PagePool* GetPool() { return &pool_; }

 private:
  PageRef GetMetaPage();

 private:
  friend class Recovery;
  PagePool pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
