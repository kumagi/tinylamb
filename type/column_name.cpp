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

//
// Created by kumagi on 22/07/07.
//
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
  return schema.empty() ? name : schema + "." + name;
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
    const tinylamb::ColumnName& c) const {
  uint64_t result = std::hash<std::string_view>()(c.schema);
  result = std::hash<std::string_view>()(c.name);
  return result;
}
