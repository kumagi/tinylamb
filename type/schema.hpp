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

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include <cassert>
#include <unordered_set>
#include <vector>

#include "common/log_message.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Schema {
 public:
  Schema() = default;
  Schema(const Schema&) = default;
  Schema(Schema&&) = default;
  Schema& operator=(const Schema&) = default;
  Schema& operator=(Schema&&) = default;
  ~Schema() = default;

  Schema(std::string_view schema_name, std::vector<Column> columns);
  [[nodiscard]] slot_t ColumnCount() const { return columns_.size(); }
  [[nodiscard]] std::string_view Name() const { return name_; }
  [[nodiscard]] const Column& GetColumn(size_t idx) const {
    return columns_[idx];
  }
  [[nodiscard]] std::unordered_set<ColumnName> ColumnSet() const;
  [[nodiscard]] int Offset(const ColumnName& name) const;

  Schema operator+(const Schema& rhs) const;
  bool operator==(const Schema& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const Schema& s);
  friend Encoder& operator<<(Encoder& a, const Schema& sc);
  friend Decoder& operator>>(Decoder& a, Schema& sc);

 private:
  std::string name_;
  std::vector<Column> columns_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SCHEMA_HPP
