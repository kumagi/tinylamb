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

#include <atomic>
#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "common/log_message.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Schema {
 public:
  // Value type: every instance owns its name and column data (copies are deep
  // copies). Copying from a reference whose pointee was destroyed therefore
  // faults right here in the copy constructor -- if this line crashes, the
  // *source* Schema (or its owning Table) had a dangling lifetime.
  Schema();
  Schema(const Schema& other);
  Schema(Schema&& other) noexcept;
  Schema& operator=(const Schema& other);
  Schema& operator=(Schema&& other) noexcept;
  ~Schema() = default;

  Schema(std::string_view schema_name, std::vector<Column> columns);
  [[nodiscard]] slot_t ColumnCount() const {
    return static_cast<slot_t>(columns_.size());
  }
  [[nodiscard]] std::string_view Name() const { return name_; }
  [[nodiscard]] std::uint64_t LookupIdentity() const { return identity_; }
  [[nodiscard]] const Column& GetColumn(size_t idx) const {
    return columns_[idx];
  }
  [[nodiscard]] std::unordered_set<ColumnName> ColumnSet() const;
  [[nodiscard]] int Offset(const ColumnName& name) const;

  // A copy with the relation name (and every column's qualifier) replaced.
  // ALTER TABLE ... RENAME TO uses this to keep the catalog-serialized
  // schema self-consistent; page data stores no names, so no row rewrite
  // follows.
  [[nodiscard]] Schema WithName(std::string_view name) const;
  // A copy with the column at |offset| renamed.  ALTER TABLE ... RENAME
  // COLUMN; statistics keys are column-indexed and stay valid.
  [[nodiscard]] Schema WithRenamedColumn(size_t offset,
                                         std::string_view name) const;

  Schema operator+(const Schema& rhs) const;
  bool operator==(const Schema& rhs) const;
  friend std::ostream& operator<<(std::ostream& o, const Schema& s);
  friend Encoder& operator<<(Encoder& a, const Schema& sc);
  friend Decoder& operator>>(Decoder& e, Schema& sc);

 private:
  static std::uint64_t NextLookupIdentity();

  std::string name_;
  std::vector<Column> columns_;
  std::uint64_t identity_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SCHEMA_HPP
