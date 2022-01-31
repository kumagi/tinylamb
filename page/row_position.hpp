//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_ROW_POSITION_HPP
#define TINYLAMB_ROW_POSITION_HPP

#include <cstring>
#include <ostream>

#include "common/constants.hpp"

namespace tinylamb {

struct RowPosition {
  // Returns invalid position.
  RowPosition() = default;
  RowPosition(page_id_t p, slot_t s) : page_id(p), slot(s) {}

  // The page where the row exists.
  page_id_t page_id = ~0LLU;

  // n-th row in the page.
  slot_t slot = ~0;

  [[nodiscard]] bool IsValid() const { return page_id != ~0LLU && slot != ~0; }

  friend std::ostream& operator<<(std::ostream& o, const RowPosition& p) {
    o << "{" << p.page_id << ": " << p.slot << "}";
    return o;
  }
  bool operator==(const RowPosition& rhs) const {
    return page_id == rhs.page_id && slot == rhs.slot;
  }

  static constexpr size_t Size() { return sizeof(page_id) + sizeof(slot); }
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::RowPosition> {
 public:
  uint64_t operator()(const tinylamb::RowPosition& target) const {
    return std::hash<tinylamb::page_id_t>()(target.page_id) +
           std::hash<tinylamb::slot_t>()(target.slot);
  }
};

}  // namespace std

#endif  // TINYLAMB_ROW_POSITION_HPP
