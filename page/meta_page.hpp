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

#ifndef TINYLAMB_META_PAGE_HPP
#define TINYLAMB_META_PAGE_HPP

#include <cstdint>

#include "common/constants.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"

namespace tinylamb {

class Transaction;
class Page;
class PagePool;

class MetaPage {
 public:
  [[nodiscard]] page_id_t MaxPageCountForTest() const { return max_page_count; }

 private:
  void Initialize() {
    max_page_count = 0;
    first_free_page = 0;
  }

  PageRef AllocateNewPage(Transaction& txn, PagePool& pool,
                          PageType new_page_type);
  void DestroyPage(Transaction& txn, Page* target);

  // Note that all member of this class is private.
  // Only Page class can access these members.
  friend class Page;
  friend std::hash<tinylamb::MetaPage>;

  uint64_t first_free_page;
  void Dump(std::ostream& o, int) const;
  uint64_t max_page_count;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::MetaPage> {
 public:
  uint64_t operator()(const tinylamb::MetaPage& m);
};
}  // namespace std

#endif  // TINYLAMB_META_PAGE_HPP
