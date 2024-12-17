/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "debug.hpp"

#include <cstddef>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

namespace tinylamb {
std::string Hex(std::string_view in) {
  std::stringstream s;
  for (size_t i = 0; i < in.size(); ++i) {
    if (0 < i) {
      s << " ";
    }
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
