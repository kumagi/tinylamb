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

#ifndef TINYLAMB_TYPE_HPP
#define TINYLAMB_TYPE_HPP

#include <string>

#include "common/decoder.hpp"
#include "common/encoder.hpp"

namespace tinylamb {

enum class TypeTag {
  kInvalid,
  kInteger,
  kBigInt,
  kDouble,
  kVarChar,
  kBinaryExp,
  kColumnValue,
  kConstantValue,
  kUnaryExp,
  kAggregateExp,
  kCaseExp,
  kInExp,
  kFunctionCallExp,
};

class Type {
 public:
  Type() : type_(TypeTag::kInvalid) {}
  Type(TypeTag type) : type_(type) {}
  TypeTag GetType() const { return type_; }
  bool IsValid() const { return type_ != TypeTag::kInvalid; }
  bool IsVariableLength() const { return type_ == TypeTag::kVarChar; }
  size_t Size() const;
  std::string ToString() const;

  friend Encoder& operator<<(Encoder& e, const Type& t) {
    e << static_cast<uint8_t>(t.type_);
    return e;
  }

  friend Decoder& operator>>(Decoder& d, Type& t) {
    uint8_t type_val;
    d >> type_val;
    t.type_ = static_cast<TypeTag>(type_val);
    return d;
  }

 private:
  TypeTag type_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TYPE_HPP
