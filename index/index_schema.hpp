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

#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"

namespace tinylamb {
struct Row;
class Encoder;
class Decoder;

enum class IndexMode : bool { kUnique = true, kNonUnique = false };
std::ostream& operator<<(std::ostream& o, const IndexMode& mode);

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
  [[nodiscard]] bool IsUnique() const { return mode_ == IndexMode::kUnique; }
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
