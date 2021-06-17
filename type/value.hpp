//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_VALUE_HPP
#define TINYLAMB_VALUE_HPP

#include <cstdint>

namespace tinylamb {

class Value {
 public:
  Value(int64_t int_val) {
    value.int_value = int_val;
    length = 8;
  }
  Value(std::string_view varchar_val) {
    value.varchar_value = varchar_val.data();
    length = varchar_val.length;
  }
  union {
    int64_t int_value;
    const char* varchar_value;
  } value;
  size_t length;
};

}  // namespace tinylamb

#endif  // TINYLAMB_VALUE_HPP
