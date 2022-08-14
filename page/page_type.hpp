//
// Created by kumagi on 2021/12/22.
//

#ifndef TINYLAMB_PAGE_TYPE_HPP
#define TINYLAMB_PAGE_TYPE_HPP

#include <cstdint>
#include <iosfwd>

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

std::ostream& operator<<(std::ostream& o, const PageType& type);
Encoder& operator<<(Encoder& e, const PageType& type);
Decoder& operator>>(Decoder& e, PageType& type);

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_TYPE_HPP
