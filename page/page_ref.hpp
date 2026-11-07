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

#include <cassert>
#include <mutex>
#include <ostream>
#include <shared_mutex>

namespace tinylamb {

class Page;
class RowPage;
class PageManager;

class PageRef final {
 private:
  PageRef(Page* page, std::shared_mutex* page_lock, bool shared);

  PageRef() = default;

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
  [[nodiscard]] bool IsNull() const { return page_ == nullptr; }
  Page* get() { return page_; }
  [[nodiscard]] const Page* get() const { return page_; }

  ~PageRef();

  PageRef(const PageRef&) = delete;
  PageRef(PageRef&& o) noexcept
      : page_(o.page_),
        exclusive_page_lock_(std::move(o.exclusive_page_lock_)),
        shared_page_lock_(std::move(o.shared_page_lock_)) {
    o.page_ = nullptr;
  }
  PageRef& operator=(const PageRef&) = delete;
  PageRef& operator=(PageRef&& o) noexcept {
    if (this != &o) {
      if (page_ != nullptr) {
        PageUnlock();
      }
      page_ = o.page_;
      exclusive_page_lock_ = std::move(o.exclusive_page_lock_);
      shared_page_lock_ = std::move(o.shared_page_lock_);
      o.page_ = nullptr;
    }
    return *this;
  }
  bool operator==(const PageRef& r) const { return page_ == r.page_; }
  [[nodiscard]] bool IsValid() const { return page_ != nullptr; }
  bool operator!=(const PageRef& r) const { return !operator==(r); }
  friend std::ostream& operator<<(std::ostream& o, const PageRef& p);

 private:
  friend class PageManager;
  Page* page_{nullptr};
  std::unique_lock<std::shared_mutex> exclusive_page_lock_;
  std::shared_lock<std::shared_mutex> shared_page_lock_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_REF_HPP
