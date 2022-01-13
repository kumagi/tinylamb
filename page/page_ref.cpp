//
// Created by kumagi on 2021/11/14.
//
#include "page/page_ref.hpp"

#include "page/page.hpp"
#include "page/page_pool.hpp"

namespace tinylamb {

void PageRef::PageUnlock() {
  assert(page_);
  assert(pool_);
  assert(hold_lock_);
  pool_->PageUnlock(page_->page_id);
}

RowPage& PageRef::GetRowPage() {
  assert(page_->type == PageType::kRowPage);
  return page_->body.row_page;
}

FreePage& PageRef::GetFreePage() {
  assert(page_->type == PageType::kFreePage);
  return page_->body.free_page;
}

// Precondition: page is locked.
PageRef::PageRef(PagePool* src, Page* page)
    : pool_(src), page_(page), hold_lock_(true) {
  // LOG(TRACE) << this << " pin: " << page_->PageId();
}

PageRef::~PageRef() {
  if (page_) {
    if (hold_lock_) {
      pool_->PageUnlock(page_->PageId());
    }
    pool_->Unpin(page_->PageId());
  }
}
std::ostream& operator<<(std::ostream& o, const PageRef& p) {
  o << "{Ref: " << p.page_->PageId() << "}";
  return o;
}

}  // namespace tinylamb