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

// Fuzz Value-level memcomparable encode->decode roundtrip stability.
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "table/fuzz/fuzz_helpers.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {

int FuzzMemcmpOneInput(const uint8_t* data, size_t size) {
  if (size < 3) return 0;
  const Schema sc = MakeSchemaFromByte(data[0]);
  (void)sc;
  const auto vt = static_cast<uint8_t>(static_cast<uint8_t>(data[1] % 3) + 1);

  std::vector<Value> vals;
  const std::string payload(reinterpret_cast<const char*>(data + 2), size - 2);
  switch (static_cast<ValueType>(vt)) {
    case ValueType::kInt64: {
      int64_t v = 0;
      for (size_t i = 0; i < 8 && i < size - 2; ++i) {
        v = static_cast<int64_t>((static_cast<uint64_t>(v) << 8) | data[2 + i]);
      }
      vals.emplace_back(v);
      break;
    }
    case ValueType::kDouble: {
      double d = 0.0;
      std::memcpy(&d, data + 2, sizeof(d) < size - 2 ? sizeof(d) : size - 2);
      vals.emplace_back(d);
      break;
    }
    case ValueType::kVarChar:
      vals.emplace_back(std::string(payload));
      break;
    default:
      return 0;
  }

  for (const auto& v : vals) {
    Row r{v};
    auto enc = r.EncodeMemcomparableFormat();
    Row out;
    out.DecodeMemcomparableFormatBounded(enc);
    if (out.Count() != r.Count()) std::abort();
    if (!(out[0] == v)) std::abort();
  }
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzMemcmpOneInput(data, size);
}
#endif
