/**
 * Copyright 2026 KUMAZAKI Hiroki
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

// Fuzz Row::DeserializeBounded + memcomparable roundtrip with adversarial
// schemas.
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string_view>

#include "table/fuzz/fuzz_helpers.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb::fuzz {

int FuzzRowCodecOneInput(const uint8_t* data, size_t size) {
  if (size < 2) return 0;
  const Schema sc = MakeSchemaFromByte(data[0]);
  const uint8_t mode = data[1];

  int rejections = 0;

  // (1) Deserialize arbitrary bytes against the derived schema.
  if ((mode & 0x1) != 0) {
    try {
      Row out;
      out.DeserializeBounded(reinterpret_cast<const char*>(data + 2), size - 2,
                             sc);
      if (out.Count() != sc.ColumnCount()) {
        std::abort();
      }
    } catch (const std::runtime_error&) {
      ++rejections;
    } catch (const std::exception&) {
      ++rejections;
    }
  }

  // (2) Memcomparable decode of an arbitrary byte string must terminate and
  // stay in-bounds.
  if ((mode & 0x2) != 0) {
    try {
      const std::string_view src(reinterpret_cast<const char*>(data + 2),
                                 size - 2);
      Row out;
      out.DecodeMemcomparableFormatBounded(src);
    } catch (const std::runtime_error&) {
      ++rejections;
    } catch (const std::exception&) {
      ++rejections;
    }
  }
  static_cast<void>(rejections);
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzRowCodecOneInput(data, size);
}
#endif
