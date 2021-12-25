#ifndef TINYLAMB_FREE_PAGE_HPP
#define TINYLAMB_FREE_PAGE_HPP

#include "constants.hpp"
#include "log_message.hpp"

namespace tinylamb {

struct FreePage {
  void Initialize() { next_free_page = 0; }
  char* FreeBody() { return reinterpret_cast<char*>(&next_free_page + 1); }
  static constexpr size_t FreeBodySize() {
    return kPageBodySize - sizeof(FreePage);
  }
  uint64_t next_free_page;
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
