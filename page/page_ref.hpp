//
// Created by kumagi on 2021/11/14.
//

#ifndef TINYLAMB_PAGE_REF_HPP
#define TINYLAMB_PAGE_REF_HPP
#include <assert.h>

#include <mutex>

#include "common/log_message.hpp"

namespace tinylamb {

class PagePool;
class Page;
class MetaPage;
class RowPage;
class FreePage;

class PageRef final {
 private:
  // Precondition: page is locked.
  PageRef(PagePool* src, Page* page, std::mutex* page_lock)
      : pool_(src), page_(page), page_lock_(*page_lock) {}

  PageRef() : pool_(nullptr), page_(nullptr) {}

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
  void Swap(PageRef& other) {
    std::swap(pool_, other.pool_);
    std::swap(page_, other.page_);
  }

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef(PageRef&& o) noexcept
      : pool_(o.pool_), page_(o.page_), page_lock_(std::move(o.page_lock_)) {
    o.pool_ = nullptr;
    o.page_ = nullptr;
  }
  PageRef& operator=(const PageRef&) = delete;
  PageRef& operator=(PageRef&& o) noexcept {
    PageUnlock();
    pool_ = o.pool_;
    page_ = o.page_;
    page_lock_ = std::move(o.page_lock_);
    return *this;
  }
  bool operator==(const PageRef& r) const {
    return pool_ == r.pool_ && page_ == r.page_;
  }
  [[nodiscard]] bool IsValid() const { return pool_ != nullptr; }
  bool operator!=(const PageRef& r) const { return !operator==(r); }
  friend std::ostream& operator<<(std::ostream& o, const PageRef& p);

 private:
  friend class PagePool;
  friend class PageManager;
  friend class FullScanIterator;
  PagePool* pool_ = nullptr;
  Page* page_ = nullptr;
  std::unique_lock<std::mutex> page_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
