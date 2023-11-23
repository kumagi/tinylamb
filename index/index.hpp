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

#ifndef TINYLAMB_INDEX_HPP
#define TINYLAMB_INDEX_HPP

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/serdes.hpp"
#include "index/index_schema.hpp"

namespace tinylamb {
struct Row;
class Encoder;
class Decoder;

class Index {
 public:
  Index() = default;
  [[nodiscard]] std::string GenerateKey(const Row& row) const;
  Index(std::string_view name, std::vector<slot_t> key, page_id_t pid,
        std::vector<slot_t> include = {}, IndexMode mode = IndexMode::kUnique)
      : sc_(name, std::move(key), std::move(include), mode), pid_(pid) {}
  [[nodiscard]] bool IsUnique() const { return sc_.IsUnique(); }
  [[nodiscard]] page_id_t Root() const { return pid_; }
  [[nodiscard]] std::unordered_set<slot_t> CoveredColumns() const;
  friend Encoder& operator<<(Encoder& a, const Index& idx);
  friend Decoder& operator>>(Decoder& e, Index& idx);
  bool operator==(const Index& rhs) const = default;
  void Dump(std::ostream& o) const;
  friend std::ostream& operator<<(std::ostream& o, const Index& rhs);

  IndexSchema sc_;
  page_id_t pid_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_HPP
