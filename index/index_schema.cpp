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
  std::string key;
  for (const auto& k : key_) {
    key += row[k].EncodeMemcomparableFormat();
  }
  return key;
}

Encoder& operator<<(Encoder& a, const IndexSchema& idx) {
  a << idx.name_ << idx.key_ << idx.include_
    << (idx.mode_ == IndexMode::kUnique);
  return a;
}

Decoder& operator>>(Decoder& e, IndexSchema& idx) {
  bool mode = false;
  e >> idx.name_ >> idx.key_ >> idx.include_ >> mode;
  idx.mode_ = static_cast<IndexMode>(mode);
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