//
// Created by kumagi on 2022/02/10.
//

#include <cstddef>
#include <cstdint>
#include <iostream>

#include "type/value.hpp"

using tinylamb::Value;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2) return 0;
  for (int i = 1; i < size - 1; ++i) {
    std::string left(reinterpret_cast<const char*>(data), i);
    std::string right(reinterpret_cast<const char*>(data + i), size - i);
    Value l(std::move(left)), r(std::move(right));
    std::string encoded_left = l.EncodeMemcomparableFormat(),
                encoded_right = r.EncodeMemcomparableFormat();
    if (left < right) {
      if (encoded_right <= encoded_left) __builtin_trap();
    } else if (right < left) {
      if (encoded_left <= encoded_right) __builtin_trap();
    }
  }
  return 0;
}
