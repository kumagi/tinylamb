//
// Created by kumagi on 2021/05/29.
//

#ifndef TINYLAMB_ROW_POSITION_HPP
#define TINYLAMB_ROW_POSITION_HPP

namespace tinylamb {

struct RowPosition {
  // The page where the row exists.
  uint64_t page_id;

  // n-th row in the page.
  uint16_t slot;
};

}  // namespace tinylamb

namespace std {
class hash<tinylamb::RowPosition> {
  uint64_t operator()(const tinylamb::RowPosition& target) {
    return std::hash<uint64_t>()(target.page_id) +
           std::hash<uint16_t>()(target.slot);
  }
};
}  // namespace std

#endif  // TINYLAMB_ROW_POSITION_HPP
