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

#include <ostream>
#include <thread>

#include "page/page.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

PageRef::PageRef(Page* page, std::shared_mutex* page_lock, bool shared)
    : page_(page) {
  if (page_lock != nullptr) {
    if (shared) {
      shared_page_lock_ = std::shared_lock<std::shared_mutex>(*page_lock);
    } else {
      exclusive_page_lock_ = std::unique_lock<std::shared_mutex>(*page_lock);
      if (page_ != nullptr) {
        page_->SetExclusiveOwner(std::this_thread::get_id());
      }
    }
  }
}

RowPage& PageRef::GetRowPage() { return page_->body.row_page; }

void PageRef::PageUnlock() {
  if (exclusive_page_lock_.owns_lock()) {
    if (page_ != nullptr) {
      page_->ClearExclusiveOwner();
    }
    exclusive_page_lock_.unlock();
  }
  if (shared_page_lock_.owns_lock()) {
    shared_page_lock_.unlock();
  }
  page_ = nullptr;
}

PageRef::~PageRef() {
  if (page_ != nullptr) {
    PageUnlock();
  }
}

std::ostream& operator<<(std::ostream& o, const PageRef& p) {
  if (p.IsNull()) {
    o << "PageRef(NULL)";
  } else {
    o << *p.page_;
  }
  return o;
}

}  // namespace tinylamb
