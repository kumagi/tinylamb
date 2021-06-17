//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include "../macro.hpp"
#include "value_type.hpp"

namespace tinylamb {

struct Column {
  Column() = delete;
  Column(const Column&) = delete;
  Column(Column&&) = delete;
  Column& operator=(const Column&) = delete;
  Column& operator=(Column&&) = delete;
  friend std::ostream& operator<<(std::ostream& o, const Column& c) {
    switch (c.type) {
      case ValueType::kUnknown:
        o << "(Unknown type)";
        break;
      case ValueType::kInt64:
        o << "int64";
        break;
      case ValueType::kVarChar:
        o << "varchar(" << c.length << ")";
        break;
    }
    return o;
  }
  ValueType type = ValueType::kUnknown;
  std::string name;
  size_t length;
};

class Schema {
  MAPPING_ONLY(Schema);
  size_t RowSize() const {
    size_t ans = 0;
    for (uint16_t i = 0; i < column_count; ++i) {
      ans += columns_[i].length;
    }
    return ans;
  }
  uint16_t column_count;
  Column columns_[0];
};

}  // namespace tinylamb

#endif  // TINYLAMB_SCHEMA_HPP
