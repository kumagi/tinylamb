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


#include "type/constraint.hpp"

#include <cstring>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/value.hpp"

namespace tinylamb {

size_t Constraint::Size() const {
  switch (ctype) {
    case kDefault:
    case kForeign:
    case kCheck:
      return sizeof(ctype) + sizeof(ValueType) + value.Size();

    default:
      return sizeof(ctype);
  }
}

bool Constraint::operator==(const Constraint& rhs) const {
  switch (ctype) {
    case kDefault:
      return rhs.ctype == kDefault && value == rhs.value;

    default:
      return ctype == rhs.ctype;
  }
}

std::ostream& operator<<(std::ostream& o, const Constraint& c) {
  switch (c.ctype) {
    case tinylamb::Constraint::kNothing:
      o << "(No constraint)";
      break;
    case tinylamb::Constraint::kNotNull:
      o << "NOT NULL";
      break;
    case tinylamb::Constraint::kUnique:
      o << "UNIQUE";
      break;
    case tinylamb::Constraint::kPrimaryKey:
      o << "PRIMARY KEY";
      break;
    case tinylamb::Constraint::kIndex:
      o << "INDEX";
      break;

    case tinylamb::Constraint::kDefault:
      o << "DEFAULT(" << c.value << ")";
      break;
    case tinylamb::Constraint::kForeign:
      o << "FOREIGN(" << c.value << ")";
      break;
    case tinylamb::Constraint::kCheck:
      o << "CHECK(" << c.value << ")";
      break;
  }
  return o;
}

Encoder& operator<<(Encoder& a, const Constraint& c) {
  a << (uint8_t)c.ctype << c.value;
  return a;
}

Decoder& operator>>(Decoder& e, Constraint& c) {
  e >> (uint8_t&)c.ctype >> c.value;
  return e;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Constraint>::operator()(
    const tinylamb::Constraint& c) const {
  uint64_t result = std::hash<uint8_t>()(c.ctype);
  switch (c.ctype) {
    case tinylamb::Constraint::kDefault:
    case tinylamb::Constraint::kForeign:
    case tinylamb::Constraint::kCheck:
      result += std::hash<tinylamb::Value>()(c.value);
      break;
    default:
      break;
  }
  return result;
}