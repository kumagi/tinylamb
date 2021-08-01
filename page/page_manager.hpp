#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include "log_message.hpp"
#include "page/page.hpp"
#include "page/page_pool.hpp"

namespace tinylamb {

class Logger;
class Recovery;
class MetaPage;
class Transaction;

class PageManager {
  static constexpr uint64_t kMetaPageId = 0;

 public:
  PageManager(std::string_view name, size_t capacity);

  Page* GetPage(uint64_t page_id);

  Page* AllocateNewPage(Transaction& txn, std::string_view header);

  // Logically delete the page.
  void DestroyPage(Transaction& txn, Page* target);

  bool Unpin(size_t page_id) { return pool_.Unpin(page_id); }

 private:
  MetaPage& GetMetaPage();

  void UnpinMetaPage() { pool_.Unpin(kMetaPageId); }

 private:
  friend class Recovery;
  PagePool pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
