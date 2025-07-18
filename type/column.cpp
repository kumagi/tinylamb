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
#include <iostream>
#include <string_view>

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
  a << c.col_name_ << c.type_ << c.constraint_;
  return a;
}

Decoder& operator>>(Decoder& e, Column& c) {
  e >> c.col_name_ >> c.type_ >> c.constraint_;
  return e;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const {
  uint64_t result = std::hash<tinylamb::ColumnName>()(c.Name());
  result += std::hash<tinylamb::ValueType>()(c.Type());
  result += std::hash<tinylamb::Constraint>()(c.GetConstraint());
  return result;
}
