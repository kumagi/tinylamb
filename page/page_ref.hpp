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

class PageRef {
 private:
  PageRef(PagePool* src, Page* page);

 public:
  Page& operator*() { return *page_; }
  const Page& operator*() const { return *page_; }
  Page* operator->() { return page_; }
  const Page* operator->() const { return page_; }

  RowPage& GetRowPage();
  MetaPage& GetMetaPage();
  FreePage& GetFreePage();
  [[nodiscard]] bool IsNull() const { return page_ == nullptr; }
  Page* get() { return page_; }
  [[nodiscard]] const Page* get() const { return page_; }

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef(PageRef&& o) noexcept : pool_(o.pool_), page_(o.page_) {
    // LOG(TRACE) << &o << " -> moved: " << this;
    o.pool_ = nullptr;
    o.page_ = nullptr;
  }
  PageRef& operator=(const PageRef&) = delete;
  PageRef& operator=(PageRef&&) = delete;
  bool operator==(const PageRef& r) const {
    return pool_ == r.pool_ && page_ == r.page_;
  }
  bool operator!=(const PageRef& r) const { return !operator==(r); }


 private:
  friend class PagePool;
  PagePool* pool_ = nullptr;
  Page* page_ = nullptr;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
