//
// Created by kumagi on 2021/11/14.
//

#ifndef TINYLAMB_PAGE_REF_HPP
#define TINYLAMB_PAGE_REF_HPP

#include "log_message.hpp"

namespace tinylamb {

class PagePool;
class Page;
class MetaPage;
class RowPage;
class FreePage;

class PageRef final {
 private:
  PageRef(PagePool* src, Page* page);

 public:
  Page& operator*() { return *page_; }
  const Page& operator*() const { return *page_; }
  Page* operator->() { return page_; }
  const Page* operator->() const { return page_; }

  void PageUnlock();
  RowPage& GetRowPage();
  FreePage& GetFreePage();
  [[nodiscard]] bool IsNull() const { return page_ == nullptr; }
  Page* get() { return page_; }
  [[nodiscard]] const Page* get() const { return page_; }

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef(PageRef&& o) noexcept
      : pool_(o.pool_), page_(o.page_), hold_lock_(o.hold_lock_) {
    o.pool_ = nullptr;
    o.page_ = nullptr;
    o.hold_lock_ = false;
  }
  PageRef& operator=(const PageRef&) = delete;
  PageRef& operator=(PageRef&&) = delete;
  bool operator==(const PageRef& r) const {
    return pool_ == r.pool_ && page_ == r.page_ && hold_lock_ == r.hold_lock_;
  }
  bool operator!=(const PageRef& r) const { return !operator==(r); }
  friend std::ostream& operator<<(std::ostream& o, const PageRef& p);

 private:
  friend class PagePool;
  friend class PageManager;
  PagePool* pool_ = nullptr;
  Page* page_ = nullptr;
  bool hold_lock_ = false;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
