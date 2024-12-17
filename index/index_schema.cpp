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
// Created by kumagi on 22/05/05.
//

#include "index_schema.hpp"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/row.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const IndexMode& mode) {
  o << (mode == IndexMode::kUnique ? "Unique" : "NonUnique");
  return o;
}

std::string IndexSchema::GenerateKey(const Row& row) const {
  std::stringstream s;
  for (const auto& k : key_) {
    s << row[k].EncodeMemcomparableFormat();
  }
  return s.str();
}

Encoder& operator<<(Encoder& a, const IndexSchema& idx) {
  a << idx.name_ << idx.key_ << idx.include_ << (bool)idx.mode_;
  return a;
}

Decoder& operator>>(Decoder& e, IndexSchema& idx) {
  e >> idx.name_ >> idx.key_ >> idx.include_ >> (bool&)idx.mode_;
  return e;
}

std::ostream& operator<<(std::ostream& o, const IndexSchema& rhs) {
  o << rhs.name_ << " => [ Column: {";
  for (size_t i = 0; i < rhs.key_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << rhs.key_[i];
  }
  o << "}";
  if (!rhs.include_.empty()) {
    o << " Include: {";
    for (size_t i = 0; i < rhs.include_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << rhs.include_[i];
    }
    o << "}";
  }
  o << " " << rhs.mode_ << "]";
  return o;
}

}  // namespace tinylamb