//
// Created by kumagi on 2021/11/14.
//
#include "page/page_ref.hpp"

#include "page/page.hpp"
#include "page/page_pool.hpp"

namespace tinylamb {

RowPage& PageRef::GetRowPage() {
  assert(page_->type == PageType::kRowPage);
  return page_->body.row_page;
}

MetaPage& PageRef::GetMetaPage() {
  assert(page_->type == PageType::kMetaPage);
  return page_->body.meta_page;
}

FreePage& PageRef::GetFreePage() {
  assert(page_->type == PageType::kFreePage);
  return page_->body.free_page;
}

PageRef::PageRef(PagePool* src, Page* page) : pool_(src), page_(page) {
  // LOG(TRACE) << this << " pin: " << page_->PageId();
}

PageRef::~PageRef() {
  if (page_) {
    pool_->Unpin(page_->PageId());
    // LOG(TRACE) << this << " unpin: " << page_->PageId();
  }
}

}  // namespace tinylamb