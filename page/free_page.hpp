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
  void Initialize() { next_free_page = 0; }

 public:
  char* FreeBody() { return reinterpret_cast<char*>(&next_free_page + 1); }
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
