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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/row.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const IndexMode& mode) {
  switch (mode) {
    case IndexMode::kUnique:
      o << "Unique";
      return o;
    case IndexMode::kNonUnique:
      o << "NonUnique";
      return o;
    case IndexMode::kVersionedUnique:
      o << "VersionedUnique";
      return o;
  }
  // T2116 (same site as P938): a mode decoded from a corrupt record falls
  // outside the enum; the old switch emitted NOTHING and the dump read
  // "... ]" with the mode silently gone.  Label the numeric value instead.
  o << "Invalid(" << static_cast<int>(mode) << ")";
  return o;
}

std::string IndexSchema::GenerateKey(const Row& row) const {
  std::string key;
  size_t estimate = 0;
  for (const auto k : key_) {
    const Value& value = row[k];
    size_t bytes = value.IsNull() ? 1 : 9;
    if (value.type == ValueType::kVarChar) {
      if (value.IsInterval()) {
        bytes = 25;
      } else {
        const size_t length = value.AsStringView().size();
        const size_t groups =
            std::max<size_t>(1, length / 8 + (length % 8 != 0));
        if (groups > (key.max_size() - 1) / 9) {
          throw std::length_error("index key too large");
        }
        bytes = 1 + groups * 9;
      }
    }
    if (bytes > key.max_size() - estimate) {
      throw std::length_error("index key too large");
    }
    estimate += bytes;
  }
  key.reserve(estimate);
  for (const auto& k : key_) {
    key += row[k].EncodeMemcomparableFormat();
  }
  return key;
}

Encoder& operator<<(Encoder& a, const IndexSchema& idx) {
  a << idx.name_ << idx.key_ << idx.include_ << static_cast<uint8_t>(idx.mode_);
  return a;
}

void IndexSchema::NormalizeInclude() {
  std::vector<slot_t> unique_include;
  unique_include.reserve(include_.size());
  for (const slot_t slot : include_) {
    if (std::find(key_.begin(), key_.end(), slot) == key_.end() &&
        std::find(unique_include.begin(), unique_include.end(), slot) ==
            unique_include.end()) {
      unique_include.push_back(slot);
    }
  }
  include_ = std::move(unique_include);
}

Decoder& operator>>(Decoder& e, IndexSchema& idx) {
  uint8_t mode = 0;
  e >> idx.name_ >> idx.key_ >> idx.include_ >> mode;
  idx.mode_ = static_cast<IndexMode>(mode);
  idx.NormalizeInclude();
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
