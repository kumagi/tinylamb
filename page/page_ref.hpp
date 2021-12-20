//
// Created by kumagi on 2021/11/14.
//

#ifndef TINYLAMB_PAGE_REF_HPP
#define TINYLAMB_PAGE_REF_HPP

namespace tinylamb {

class PagePool;
class Page;
class MetaPage;
class CatalogPage;
class RowPage;
class FreePage;

class PageRef {
 private:
  PageRef() {}
  PageRef(PagePool* src, Page* page) : pool_(src), page_(page) {}

 public:
  Page& operator*() { return *page_; }
  const Page& operator*() const { return *page_; }
  Page* operator->() { return page_; }
  const Page* operator->() const { return page_; }
  RowPage* AsRowPage() { return reinterpret_cast<RowPage*>(page_); }
  CatalogPage* AsCatalogPage() { return reinterpret_cast<CatalogPage*>(page_); }
  MetaPage* AsMetaPage() { return reinterpret_cast<MetaPage*>(page_); }
  FreePage* AsFreePage() { return reinterpret_cast<FreePage*>(page_); }
  bool IsNull() const { return page_ == nullptr; }
  Page* get() { return page_; }
  [[nodiscard]] const Page* get() const { return page_; }

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef& operator=(const PageRef&) = delete;
  PageRef(PageRef&&) = default;
  PageRef& operator=(PageRef&&) = delete;

 private:
  friend class PagePool;
  PagePool* pool_ = nullptr;
  Page* page_ = nullptr;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
