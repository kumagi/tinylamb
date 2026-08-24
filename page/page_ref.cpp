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

#include <cassert>
#include <ostream>

#include "page/page.hpp"
#include "page/page_pool.hpp"
#include "page_type.hpp"

namespace tinylamb {
namespace {
void ReleasePin(std::atomic<uint32_t>* pin_count, page_id_t page_id) {
  if (pin_count == nullptr) { return; }
  uint32_t pins = pin_count->load(std::memory_order_relaxed);
  while (pins != 0) {
    if (pin_count->compare_exchange_weak(pins, pins - 1,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
      return;
    }
  }
  LOG(ERROR) << "unpin underflow on page " << page_id;
}
}  // namespace

void PageRef::PageUnlock() {
  assert(page_);
  assert(pool_);
  if (exclusive_page_lock_.owns_lock()) {
    exclusive_page_lock_.unlock();
    ReleasePin(pin_count_, page_->PageID());
  } else if (shared_page_lock_.owns_lock()) {
    shared_page_lock_.unlock();
    ReleasePin(pin_count_, page_->PageID());
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
  if (p.page_ == nullptr) {
    o << "{Ref: null}";
  } else {
    o << "{Ref: " << p.page_->PageID() << "}";
  }
  return o;
}

}  // namespace tinylamb
