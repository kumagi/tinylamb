#include "page_type.hpp"

#include <ostream>

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
    case PageType::kInternalPage:
      o << "InternalPageType";
      break;
  }
  return o;
}

}  // namespace tinylamb