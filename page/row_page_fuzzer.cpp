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

#include "page/row_page_fuzzer.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 2) {
    return 0;
  }
  static tinylamb::RowPageEnvironment env;
  tinylamb::Operation op(&env);
  std::string_view input(reinterpret_cast<const char*>(data), size);
  while (!input.empty()) {
    size_t read_bytes = op.Execute(input);
    input.remove_prefix(read_bytes);
  }
  return 0;
}
