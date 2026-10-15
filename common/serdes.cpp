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

#include "common/serdes.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>

#include "common/log_message.hpp"
#include "constants.hpp"

namespace tinylamb {

// The fixed-width big-endian scalars are inline in serdes.hpp; only the
// string / bounds-checked / stream paths remain here.

size_t SerializeStringView(char* pos, std::string_view bin) {
  // bin_size_t cannot represent longer strings; truncating silently would
  // corrupt the serialized image (length prefix vs payload mismatch).
  // Callers guarantee sizes upstream (page-size / kTooBigData checks), so an
  // oversize string here is an invariant violation, not a runtime error.
  if (bin.size() > std::numeric_limits<bin_size_t>::max()) {
    LOG(FATAL) << "string too long to serialize: " << bin.size();
    abort();
  }
  const auto len = static_cast<bin_size_t>(bin.size());
  SerializeU16(pos, len);
  memcpy(pos + sizeof(len), bin.data(), bin.size());
  return sizeof(len) + bin.size();
}

size_t DeserializeStringView(const char* pos, const char* end,
                             std::string_view* out) {
  if (end == nullptr || static_cast<size_t>(end - pos) < sizeof(bin_size_t)) {
    return 0;
  }
  bin_size_t len = 0;
  DeserializeU16(pos, &len);
  if (static_cast<size_t>(end - pos) - sizeof(bin_size_t) < len) {
    return 0;
  }
  *out = {pos + sizeof(len), len};
  return sizeof(len) + len;
}

}  // namespace tinylamb
