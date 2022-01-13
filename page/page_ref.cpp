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
  page_lock_.unlock();
  page_lock_.release();
}

RowPage& PageRef::GetRowPage() {
  assert(page_->type == PageType::kRowPage);
  return page_->body.row_page;
}

FreePage& PageRef::GetFreePage() {
  assert(page_->type == PageType::kFreePage);
  return page_->body.free_page;
}

PageRef::~PageRef() {
  if (page_) {
    pool_->Unpin(page_->PageID());
    if (page_lock_.owns_lock()) {
      page_lock_.unlock();
    }
  }
}
std::ostream& operator<<(std::ostream& o, const PageRef& p) {
  o << "{Ref: " << p.page_->PageID() << "}";
  return o;
}

}  // namespace tinylamb