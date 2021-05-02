//
// Created by kumagi on 2021/12/22.
//

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
