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

#ifndef TINYLAMB_PAGE_REF_HPP
#define TINYLAMB_PAGE_REF_HPP
#include <assert.h>

#include <mutex>
#include <shared_mutex>

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
  PageRef(PagePool* src, Page* page, std::shared_mutex* page_lock, bool shared)
      : pool_(src), page_(page) {
    if (shared) {
      shared_page_lock_ = std::shared_lock<std::shared_mutex>(*page_lock);
    } else {
      exclusive_page_lock_ = std::unique_lock<std::shared_mutex>(*page_lock);
    }
  }

  PageRef() : pool_(nullptr), page_(nullptr) {}

 public:
  Page& operator*() {
    assert(page_ != nullptr);
    return *page_;
  }
  const Page& operator*() const {
    assert(page_ != nullptr);
    return *page_;
  }
  Page* operator->() {
    assert(page_ != nullptr);
    return page_;
  }
  const Page* operator->() const {
    assert(page_ != nullptr);
    return page_;
  }

  void PageUnlock();
  RowPage& GetRowPage();
  FreePage& GetFreePage();
  [[nodiscard]] bool IsNull() const { return page_ == nullptr; }
  Page* get() { return page_; }
  [[nodiscard]] const Page* get() const { return page_; }

  // Note: no Swap(). Swapping raw page pointers independently of the owned
  // locks lets a ref end up holding a latch for a different page than
  // page_, after which PageUnlock would release the wrong mutex and unpin a
  // foreign page id. Move between named PageRef variables instead.

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef(PageRef&& o) noexcept
      : pool_(o.pool_),
        page_(o.page_),
        exclusive_page_lock_(std::move(o.exclusive_page_lock_)),
        shared_page_lock_(std::move(o.shared_page_lock_)) {
    o.pool_ = nullptr;
    o.page_ = nullptr;
  }
  PageRef& operator=(const PageRef&) = delete;
  PageRef& operator=(PageRef&& o) noexcept {
    if (this != &o) {
      // Self-move must not unlock: PageUnlock on our own state followed by
      // move-assigning the std lock types from ourselves is undefined.
      if (page_ != nullptr) PageUnlock();
      pool_ = o.pool_;
      page_ = o.page_;
      exclusive_page_lock_ = std::move(o.exclusive_page_lock_);
      shared_page_lock_ = std::move(o.shared_page_lock_);
      o.pool_ = nullptr;
      o.page_ = nullptr;
    }
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
  std::unique_lock<std::shared_mutex> exclusive_page_lock_;
  std::shared_lock<std::shared_mutex> shared_page_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
