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

#ifndef TINYLAMB_FREE_PAGE_HPP
#define TINYLAMB_FREE_PAGE_HPP

#include "common/constants.hpp"
#include "common/log_message.hpp"

namespace tinylamb {

class FreePage {
  // 0 terminates the free list. It never aliases a live link target because
  // page 0 is reserved for the meta page.
  static constexpr page_id_t kEndOfFreeList = 0;

  void Initialize() { next_free_page = kEndOfFreeList; }

 public:
  char* FreeBody() {
    return reinterpret_cast<char*>(this) + sizeof(FreePage);
  }
  static constexpr size_t FreeBodySize() {
    return kPageBodySize - sizeof(FreePage);
  }

 private:
  friend class Page;
  friend class MetaPage;
  friend std::hash<tinylamb::FreePage>;

  uint64_t next_free_page;
  void Dump(std::ostream& o, int) const {
    o << "[NextFreePage: " << next_free_page << "]";
  }
  friend std::ostream& operator<<(std::ostream& o, const FreePage& f) {
    f.Dump(o, 0);
    return o;
  }
};

constexpr static uint32_t kFreeBodySize = kPageBodySize - sizeof(FreePage);

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::FreePage> {
 public:
  uint64_t operator()(const tinylamb::FreePage& p) {
    return 0xf1ee1a4e0000 + std::hash<uint64_t>()(p.next_free_page);
  }
};

}  // namespace std

#endif  // TINYLAMB_FREE_PAGE_HPP
