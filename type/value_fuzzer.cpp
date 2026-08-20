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
#include <string_view>

#include "type/value.hpp"

using tinylamb::Value;

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 2) {
    return 0;
  }
  // Split the input at every position and check that the memcomparable
  // encoding preserves the VARCHAR total order.  The source strings must be
  // compared before they are moved into the Value objects; comparing the
  // moved-from strings would silently make every comparison false.
  const std::string_view input(reinterpret_cast<const char*>(data), size);
  for (size_t i = 1; i < size - 1; ++i) {
    const std::string_view left = input.substr(0, i);
    const std::string_view right = input.substr(i);
    const Value l{std::string(left)}, r{std::string(right)};
    const std::string encoded_left = l.EncodeMemcomparableFormat();
    const std::string encoded_right = r.EncodeMemcomparableFormat();
    const int cmp = left < right ? -1 : (right < left ? 1 : 0);
    const int encoded_cmp =
        encoded_left < encoded_right ? -1 : (encoded_right < encoded_left ? 1 : 0);
    if (cmp != encoded_cmp) {
      __builtin_trap();
    }
  }
  return 0;
}
