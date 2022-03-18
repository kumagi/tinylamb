//
// Created by kumagi on 2022/02/07.
//

#include "debug.hpp"

#include <iostream>

namespace tinylamb {

std::string Hex(std::string_view in) {
  std::stringstream s;
  for (size_t i = 0; i < in.size(); ++i) {
    if (0 < i) s << " ";
    s << std::hex << std::setw(2) << std::setfill('0') << (int)(in[i] & 0xff);
  }
  return s.str();
}

}  // namespace tinylamb