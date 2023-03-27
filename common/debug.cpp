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

std::string OmittedString(std::string_view original, int length) {
  if ((size_t)length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  }
  return std::string(original);
}

std::string HeadString(std::string_view original, int length) {
  if ((size_t)length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 8) + "bytes)";
    return omitted_key;
  }
  return std::string(original);
}

}  // namespace tinylamb