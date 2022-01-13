#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include <string_view>

#include "log_message.hpp"
#include "page/page.hpp"
#include "page/page_pool.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

class Logger;
class RecoveryManager;
class MetaPage;
class Transaction;
class TransactionManager;

class PageManager {
  static constexpr page_id_t kMetaPageId = 0;

 public:
  PageManager(std::string_view db_name, size_t capacity);

  PageRef GetPage(page_id_t page_id);

  PageRef AllocateNewPage(Transaction& txn, PageType new_page_type);

  // Logically delete the page.
  void DestroyPage(Transaction& txn, Page* target);

  PagePool* GetPool() { return &pool_; }

 private:
  PageRef GetMetaPage();

 private:
  friend class RecoveryManager;
  PagePool pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
