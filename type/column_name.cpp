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

#include "type/column_name.hpp"

#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>

#include "common/decoder.hpp"
#include "common/encoder.hpp"

namespace tinylamb {
std::ostream& operator<<(std::ostream& o, const ColumnName& c) {
  o << c.ToString();
  return o;
}

std::string ColumnName::ToString() const {
  if (schema.empty()) {
    return name;
  }
  // `schema + "." + name` builds a "schema." temporary and then reallocates
  // for the append; reserve once and append in place instead.
  std::string out;
  out.reserve(schema.size() + 1 + name.size());
  out.append(schema);
  out.push_back('.');
  out.append(name);
  return out;
}

bool ColumnName::operator<(const ColumnName& rhs) const {
  return std::tie(schema, name) < std::tie(rhs.schema, rhs.name);
}

Encoder& operator<<(Encoder& a, const ColumnName& c) {
  a << c.schema << c.name;
  return a;
}

Decoder& operator>>(Decoder& e, ColumnName& c) {
  e >> c.schema >> c.name;
  return e;
}
}  // namespace tinylamb
uint64_t std::hash<tinylamb::ColumnName>::operator()(
    const tinylamb::ColumnName& c) const noexcept {
  uint64_t result = std::hash<std::string_view>()(c.schema);
  // Order-sensitive combine: += would make ("a","b") and ("b","a") collide.
  result ^= std::hash<std::string_view>()(c.name) + 0x9e3779b97f4a7c15ULL +
            (result << 6) + (result >> 2);
  return result;
}
