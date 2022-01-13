//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_ROW_POSITION_HPP
#define TINYLAMB_ROW_POSITION_HPP

#include <cstring>
#include <ostream>

#include "constants.hpp"

namespace tinylamb {

struct RowPosition {
  // Returns invalid position.
  RowPosition() = default;
  RowPosition(page_id_t p, uint16_t s) : page_id(p), slot(s) {}

  // The page where the row exists.
  page_id_t page_id = -1;

  // n-th row in the page.
  uint16_t slot = -1;

  [[nodiscard]] bool IsValid() const { return page_id != -1 && slot != -1; }

  friend std::ostream& operator<<(std::ostream& o, const RowPosition& p) {
    o << "{" << p.page_id << ": " << p.slot << "}";
    return o;
  }
  bool operator==(const RowPosition& rhs) const {
    return page_id == rhs.page_id && slot == rhs.slot;
  }

  static constexpr size_t Size() { return sizeof(page_id) + sizeof(slot); }

  size_t Serialize(char* dst) const {
    memcpy(dst, &page_id, sizeof(page_id));
    memcpy(dst + sizeof(page_id), &slot, sizeof(slot));
    return Size();
  }

  size_t Parse(const char* src) {
    memcpy(&page_id, src, sizeof(page_id));
    memcpy(&slot, src + sizeof(page_id), sizeof(slot));
    return Size();
  }
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::RowPosition> {
 public:
  uint64_t operator()(const tinylamb::RowPosition& target) const {
    return std::hash<uint64_t>()(target.page_id) +
           std::hash<uint16_t>()(target.slot);
  }
};

}  // namespace std

#endif  // TINYLAMB_ROW_POSITION_HPP
