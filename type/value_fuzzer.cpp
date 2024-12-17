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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "type/value.hpp"

using tinylamb::Value;

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 2) {
    return 0;
  }
  for (size_t i = 1; i < size - 1; ++i) {
    std::string left(reinterpret_cast<const char*>(data), i);
    std::string right(reinterpret_cast<const char*>(data + i), size - i);
    Value l(std::move(left)), r(std::move(right));
    std::string encoded_left = l.EncodeMemcomparableFormat(),
                encoded_right = r.EncodeMemcomparableFormat();
    if (left < right) {
      if (encoded_right <= encoded_left) {
        __builtin_trap();
      }
    } else if (right < left) {
      if (encoded_left <= encoded_right) {
        __builtin_trap();
      }
    }
  }
  return 0;
}
