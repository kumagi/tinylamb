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

#include "schema.hpp"

#include <cstring>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "type/column_name.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Schema::Schema(std::string_view schema_name, std::vector<Column> columns)
    : name_(schema_name), columns_(std::move(columns)) {
  for (auto& c : columns_) {
    if (c.Name().schema.empty()) {
      c.Name().schema = Name();
    }
  }
}

std::unordered_set<ColumnName> Schema::ColumnSet() const {
  std::unordered_set<ColumnName> ret;
  ret.reserve(columns_.size());
  for (const auto& c : columns_) {
    if (c.Name().name.empty()) {
      ret.emplace(Name(), c.Name().name);
    } else {
      ret.emplace(c.Name());
    }
  }
  return ret;
}

int Schema::Offset(const ColumnName& col_name) const {
  if (!col_name.schema.empty() && !Name().empty() &&
      Name() != col_name.schema) {
    return -1;
  }
  for (int i = 0; i < static_cast<int>(columns_.size()); ++i) {
    if (columns_[i].Name() == col_name) {
      return i;
    }
  }
  return -1;
}

Schema Schema::operator+(const Schema& rhs) const {
  std::vector<Column> merged = columns_;
  merged.reserve(columns_.size() + rhs.columns_.size());
  for (const auto& c : rhs.columns_) {
    merged.push_back(c);
  }
  return {"", merged};
}

std::ostream& operator<<(std::ostream& o, const Schema& s) {
  o << s.name_ << " [ ";
  for (size_t i = 0; i < s.columns_.size(); ++i) {
    if (0 < i) {
      o << " | ";
    }
    o << s.columns_[i];
  }
  o << " ]";
  return o;
}

Encoder& operator<<(Encoder& a, const Schema& sc) {
  a << sc.name_ << sc.columns_;
  return a;
}

Decoder& operator>>(Decoder& e, Schema& sc) {
  e >> sc.name_ >> sc.columns_;
  return e;
}

}  // namespace tinylamb
