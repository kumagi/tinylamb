/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "page/page_ref.hpp"

#include "page/page.hpp"
#include "page/page_pool.hpp"

namespace tinylamb {

void PageRef::PageUnlock() {
  assert(page_);
  assert(pool_);
  if (page_lock_.owns_lock()) {
    page_lock_.unlock();
    pool_->Unpin(page_->PageID());
  }
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
  if (page_ != nullptr) {
    PageUnlock();
  }
}

std::ostream& operator<<(std::ostream& o, const PageRef& p) {
  o << "{Ref: " << p.page_->PageID() << "}";
  return o;
}

}  // namespace tinylamb