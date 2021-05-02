//
// Created by kumagi on 22/08/05.
//

#ifndef TINYLAMB_ROW_POINTER_HPP
#define TINYLAMB_ROW_POINTER_HPP

#include "common/constants.hpp"
namespace tinylamb {

struct RowPointer {
  // Row start position from beginning fom this page.
  bin_size_t offset = 0;

  // Physical row size in bytes (required to get exact size for logging).
  bin_size_t size = 0;

  bool operator==(const RowPointer&) const = default;
  friend std::ostream& operator<<(std::ostream& o, const RowPointer& rp) {
    o << "{" << rp.offset << ", " << rp.size << "}";
    return o;
  }
};

constexpr static RowPointer kMinusInfinity{1, 0};
constexpr static RowPointer kPlusInfinity{2, 0};
}  // namespace tinylamb

#endif  // TINYLAMB_ROW_POINTER_HPP
