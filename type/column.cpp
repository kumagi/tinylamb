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

#include "type/column.hpp"

#include <cstdint>
#include <functional>
#include <iostream>
#include <string_view>
#include <utility>

#include "column_name.hpp"
#include "common/encoder.hpp"
#include "constraint.hpp"
#include "value_type.hpp"

namespace tinylamb {

Column::Column(ColumnName name, ValueType type, Constraint cst)
    : col_name_(std::move(name)), type_(type), constraint_(std::move(cst)) {}
Column::Column(std::string_view name, ValueType type, Constraint cst)
    : col_name_(ColumnName(name)), type_(type), constraint_(std::move(cst)) {}

std::ostream& operator<<(std::ostream& o, const Column& c) {
  o << c.col_name_;
  if (c.type_ != ValueType::kNull) {
    o << ": " << ValueTypeToString(c.type_);
  }
  if (!c.constraint_.IsNothing()) {
    o << "(" << c.constraint_ << ")";
  }
  return o;
}

Encoder& operator<<(Encoder& a, const Column& c) {
  // Keep the signed value representation while recording UINT64 in the
  // catalog's column metadata.  The high bit is not part of ValueType, so
  // existing signed column encodings remain byte-compatible.
  uint8_t encoded_type = static_cast<uint8_t>(c.type_);
  if (c.unsigned_) { encoded_type |= 0x80; }
  a << c.col_name_ << encoded_type << c.constraint_;
  return a;
}

Decoder& operator>>(Decoder& e, Column& c) {
  uint8_t encoded_type = 0;
  e >> c.col_name_ >> encoded_type >> c.constraint_;
  c.unsigned_ = (encoded_type & 0x80) != 0;
  c.type_ = static_cast<ValueType>(encoded_type & 0x7f);
  return e;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const noexcept {
  uint64_t result = std::hash<tinylamb::ColumnName>()(c.Name());
  result += std::hash<tinylamb::ValueType>()(c.Type());
  result += std::hash<bool>()(c.IsUnsigned());
  result += std::hash<tinylamb::Constraint>()(c.GetConstraint());
  return result;
}
