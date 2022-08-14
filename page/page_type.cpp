#include "page_type.hpp"

#include <ostream>

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
  }
  return o;
}

Encoder& operator<<(Encoder& e, const PageType& type) {
  e << reinterpret_cast<const uint64_t&>(type);
  return e;
}

Decoder& operator>>(Decoder& d, PageType& type) {
  auto* type_p = reinterpret_cast<uint64_t*>(&type);
  d >> *type_p;
  return d;
}

}  // namespace tinylamb