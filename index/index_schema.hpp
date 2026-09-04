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

#ifndef TINYLAMB_INDEX_SCHEMA_HPP
#define TINYLAMB_INDEX_SCHEMA_HPP

#include <ostream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {
struct Row;
class Encoder;
class Decoder;

// The on-disk value representation is part of the mode:
//  * kUnique stores one IndexValueType per key.
//  * kNonUnique stores a vector and physically removes deleted positions.
//  * kVersionedUnique stores a vector and retains deleted positions so an
//    older MVCC snapshot can still find the row version through the heap.
//
// IndexMode used to be serialized as bool.  It is deliberately an explicit
// byte now: adding the versioned representation is an on-disk format break.
enum class IndexMode : uint8_t {
  kNonUnique = 0,
  kUnique = 1,
  kVersionedUnique = 2,
};
std::ostream& operator<<(std::ostream& o, const IndexMode& mode);

// Value stored in a B+Tree leaf for one table row: where the row lives and
// which columns it contributes to the index include list.  Encoded as
// {RowPosition, Row} exactly as before the move out of table.hpp; the
// operator<</operator>> definitions still live in table.cpp.
struct IndexValueType {
  RowPosition pos;
  Row include;
  friend Encoder& operator<<(Encoder& e, const IndexValueType& v);
  friend Decoder& operator>>(Decoder& d, IndexValueType& t);
  friend std::ostream& operator<<(std::ostream& o, const IndexValueType& v) {
    o << "IndexValueType(pos=" << v.pos << ", include=" << v.include << ")";
    return o;
  }
};

class IndexSchema {
 public:
  IndexSchema() = default;
  IndexSchema(std::string_view name, std::vector<slot_t> key,
              std::vector<slot_t> include = {},
              IndexMode mode = IndexMode::kUnique)
      : name_(name),
        key_(std::move(key)),
        include_(std::move(include)),
        mode_(mode) {}
  IndexSchema(const IndexSchema&) = default;
  IndexSchema(IndexSchema&&) = default;
  IndexSchema& operator=(const IndexSchema&) = default;
  IndexSchema& operator=(IndexSchema&&) = default;
  bool operator==(const IndexSchema& rhs) const = default;

  [[nodiscard]] std::string GenerateKey(const Row& row) const;
  [[nodiscard]] bool IsUnique() const {
    return mode_ == IndexMode::kUnique || mode_ == IndexMode::kVersionedUnique;
  }
  [[nodiscard]] bool StoresSingleValue() const {
    return mode_ == IndexMode::kUnique;
  }
  [[nodiscard]] bool RetainsDeletedEntries() const {
    return mode_ == IndexMode::kVersionedUnique;
  }
  friend Encoder& operator<<(Encoder& a, const IndexSchema& idx);
  friend Decoder& operator>>(Decoder& e, IndexSchema& idx);
  friend std::ostream& operator<<(std::ostream& o, const IndexSchema& rhs);

  std::string name_;
  std::vector<slot_t> key_;
  std::vector<slot_t> include_;
  IndexMode mode_{IndexMode::kUnique};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCHEMA_HPP
