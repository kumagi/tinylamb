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

#include "page_type.hpp"

#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>

#include "common/decoder.hpp"
#include "common/encoder.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const PageType& type) {
  switch (type) {
    case PageType::kUnknown:
      o << "UnknownPageType";
      break;
    case PageType::kFreePage:
      o << "FreePageType";
      break;
    case PageType::kMetaPage:
      o << "MetaPageType";
      break;
    case PageType::kRowPage:
      o << "RowPageType";
      break;
    case PageType::kLeafPage:
      o << "LeafPageType";
      break;
    case PageType::kBranchPage:
      o << "BranchPageType";
      break;
    case PageType::kPaxPage:
      o << "PaxPageType";
      break;
    default:
      o << "(unknown)";
      break;
  }
  return o;
}

Encoder& operator<<(Encoder& e, const PageType& type) {
  e << static_cast<uint64_t>(type);
  return e;
}

Decoder& operator>>(Decoder& d, PageType& type) {
  constexpr auto kMaxValid = static_cast<uint64_t>(PageType::kPaxPage);
  uint64_t raw = 0;
  d >> raw;
  if (raw > kMaxValid) {
    // Broken images must not leak out-of-range enum values into downstream
    // switches; fail loudly instead.
    throw std::runtime_error("corrupt page type: " + std::to_string(raw));
  }
  type = static_cast<PageType>(raw);
  return d;
}

}  // namespace tinylamb
