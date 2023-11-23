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


#ifndef TINYLAMB_PAGE_TYPE_HPP
#define TINYLAMB_PAGE_TYPE_HPP

#include <cstdint>
#include <iosfwd>
#include <string>

namespace tinylamb {
class Encoder;
class Decoder;

enum class PageType : uint64_t {
  kUnknown = 0,
  kFreePage,
  kMetaPage,
  kRowPage,
  kLeafPage,
  kBranchPage,
};

inline std::string PageTypeString(enum PageType type) {
  switch (type) {
    case PageType::kFreePage:
      return "FreePage";
    case PageType::kMetaPage:
      return "MetaPage";
    case PageType::kRowPage:
      return "RowPage";
    case PageType::kLeafPage:
      return "LeafPage";
    case PageType::kBranchPage:
      return "BranchPage";
    default:
      return "(unknown)";
  }
}

std::ostream& operator<<(std::ostream& o, const PageType& type);
Encoder& operator<<(Encoder& e, const PageType& type);
Decoder& operator>>(Decoder& e, PageType& type);

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_TYPE_HPP
