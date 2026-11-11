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
#include "common/log_message.hpp"
#include "constraint.hpp"
#include "value_type.hpp"

namespace tinylamb {

Column::Column(ColumnName name, ValueType type, Constraint cst, uint8_t scale)
    : col_name_(std::move(name)),
      type_(type),
      constraint_(std::move(cst)),
      scale_(scale) {}
Column::Column(std::string_view name, ValueType type, Constraint cst,
               uint8_t scale)
    : col_name_(ColumnName(name)),
      type_(type),
      constraint_(std::move(cst)),
      scale_(scale) {}

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

bool Column::IntegerValueFits(const Value& value) const {
  if (type_ != ValueType::kInt64 || value.IsNull() ||
      value.type != ValueType::kInt64) {
    return true;
  }
  if (integer_bits_ != 8 && integer_bits_ != 16 && integer_bits_ != 32 &&
      integer_bits_ != 64) {
    return false;
  }
  const int64_t number = value.value.int_value;
  if (unsigned_) {
    if (!value.IsUnsigned() && number < 0) {
      return false;
    }
    return integer_bits_ == 64 ||
           static_cast<uint64_t>(number) < (uint64_t{1} << integer_bits_);
  }
  const uint64_t max = (uint64_t{1} << (integer_bits_ - 1)) - 1;
  if (value.IsUnsigned()) {
    const auto unsigned_number = static_cast<uint64_t>(number);
    return unsigned_number <= max;
  }
  return number >= -static_cast<int64_t>(max) - 1 &&
         std::cmp_less_equal(number, max);
}

Encoder& operator<<(Encoder& a, const Column& c) {
  // Bits 0..2 encode ValueType, bits 4..5 the declared integer width,
  // and bit 7 unsignedness. Existing 64-bit column encodings stay unchanged.
  auto encoded_type = static_cast<uint8_t>(c.type_);
  if (c.unsigned_) {
    encoded_type |= 0x80;
  }
  if (c.interval_) {
    encoded_type |= 0x08;
  }
  const uint8_t width = c.integer_bits_ == 8    ? 1
                        : c.integer_bits_ == 16 ? 2
                        : c.integer_bits_ == 32 ? 3
                                                : 0;
  encoded_type |= static_cast<uint8_t>(width << 4);
  a << c.col_name_ << encoded_type << c.constraint_;
  return a;
}

Decoder& operator>>(Decoder& e, Column& c) {
  uint8_t encoded_type = 0;
  e >> c.col_name_ >> encoded_type >> c.constraint_;
  c.unsigned_ = (encoded_type & 0x80) != 0;
  c.interval_ = (encoded_type & 0x08) != 0;
  const uint8_t raw_type = encoded_type & 0x07;
  const uint8_t width = (encoded_type >> 4) & 0x03;
  c.integer_bits_ = width == 0 ? 64 : static_cast<uint8_t>(4U << width);
  if (raw_type > static_cast<uint8_t>(ValueType::kArray) ||
      (encoded_type & 0x40) != 0 ||
      (c.interval_ && raw_type != static_cast<uint8_t>(ValueType::kVarChar)) ||
      (width != 0 && raw_type != static_cast<uint8_t>(ValueType::kInt64))) {
    // Corrupted persisted schema: sticky decoder failure so the enclosing
    // Schema/Table decode surfaces kCorrupt instead of a throw.
    e.Fail();
    c.type_ = ValueType::kNull;
    return e;
  }
  c.type_ = static_cast<ValueType>(raw_type);
  return e;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const noexcept {
  uint64_t result = std::hash<tinylamb::ColumnName>()(c.Name());
  result += std::hash<tinylamb::ValueType>()(c.Type());
  result += std::hash<bool>()(c.IsUnsigned());
  result += std::hash<bool>()(c.IsInterval());
  result += std::hash<uint8_t>()(c.IntegerBits());
  result += std::hash<tinylamb::Constraint>()(c.GetConstraint());
  return result;
}
