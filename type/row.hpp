//
// Created by kumagi on 2019/09/08.
//

#ifndef TINYLAMB_ROW_HPP
#define TINYLAMB_ROW_HPP

#include <cstring>
#include <ostream>

#include "row_position.hpp"

namespace tinylamb {

struct Row {
  Row(void* d, size_t l, RowPosition p)
      : data(reinterpret_cast<uint8_t*>(d)), length(l), pos(p), owned(false) {}

  Row& operator=(const Row& orig) {
    length = orig.length;
    owned = orig.owned;
    if (orig.owned) {
      data = new uint8_t[orig.length];
      memcpy(data, orig.data, orig.length);
      owned = true;
    } else {
      data = orig.data;
    }
    return *this;
  }

  ~Row() {
    if (owned) {
      delete[] data;
    }
  }

  friend std::ostream& operator<<(std::ostream& o, const Row& r) {
    if (r.pos.IsValid()) {
      o << r.pos << ": ";
    }
    return o;
  }

  uint8_t* data = nullptr;
  size_t length = 0;
  RowPosition pos;  // Origin of the row, if exists.
  bool owned = false;
};

}  // namespace tinylamb

#endif // TINYLAMB_ROW_HPP
