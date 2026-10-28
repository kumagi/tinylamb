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
#include <ostream>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/column_name.hpp"

namespace tinylamb {

namespace {
std::atomic<std::uint64_t> next_lookup_identity{1};
}

std::uint64_t Schema::NextLookupIdentity() {
  return next_lookup_identity.fetch_add(1, std::memory_order_relaxed);
}

Schema::Schema() : identity_(NextLookupIdentity()) {}

Schema::Schema(const Schema& other)
    : name_(other.name_),
      columns_(other.columns_),
      identity_(NextLookupIdentity()) {}

Schema::Schema(Schema&& other) noexcept
    : name_(std::move(other.name_)),
      columns_(std::move(other.columns_)),
      identity_(NextLookupIdentity()) {}

Schema& Schema::operator=(const Schema& other) {
  if (this != &other) {
    name_ = other.name_;
    columns_ = other.columns_;
    identity_ = NextLookupIdentity();
  }
  return *this;
}

Schema& Schema::operator=(Schema&& other) noexcept {
  if (this != &other) {
    name_ = std::move(other.name_);
    columns_ = std::move(other.columns_);
    identity_ = NextLookupIdentity();
  }
  return *this;
}

Schema::Schema(std::string_view schema_name, std::vector<Column> columns)
    : name_(schema_name),
      columns_(std::move(columns)),
      identity_(NextLookupIdentity()) {
  for (auto& c : columns_) {
    if (c.Name().schema.empty()) {
      c.Name().schema = Name();
    }
  }
}

bool Schema::operator==(const Schema& rhs) const {
  return name_ == rhs.name_ && columns_ == rhs.columns_;
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

Schema Schema::WithName(std::string_view name) const {
  Schema renamed;
  renamed.name_ = std::string(name);
  renamed.columns_ = columns_;
  for (Column& column : renamed.columns_) {
    column.Name() = ColumnName(std::string_view(name), column.Name().name);
  }
  return renamed;
}

Schema Schema::WithRenamedColumn(size_t offset, std::string_view name) const {
  Schema renamed(*this);
  if (offset >= renamed.columns_.size()) {
    return renamed;
  }
  renamed.columns_[offset].Name() =
      ColumnName(std::string_view(renamed.name_), name);
  return renamed;
}

int Schema::Offset(const ColumnName& col_name) const {
  // SQL identifiers are case-insensitive unless quoted; ColumnName does not
  // carry quotedness, so resolution folds case everywhere.
  auto equals = [](std::string_view left, std::string_view right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
      const char lc = left[i] >= 'A' && left[i] <= 'Z'
                          ? static_cast<char>(left[i] + ('a' - 'A'))
                          : left[i];
      const char rc = right[i] >= 'A' && right[i] <= 'Z'
                          ? static_cast<char>(right[i] + ('a' - 'A'))
                          : right[i];
      if (lc != rc) {
        return false;
      }
    }
    return true;
  };
  if (!col_name.schema.empty() && !Name().empty() &&
      !equals(Name(), col_name.schema)) {
    return -1;
  }
  for (size_t i = 0; i < columns_.size(); ++i) {
    const ColumnName& cn = columns_[i].Name();
    if (col_name.schema.empty()) {
      if (equals(cn.name, col_name.name)) {
        return static_cast<int>(i);
      }
    } else if (equals(cn.name, col_name.name) &&
               equals(cn.schema, col_name.schema)) {
      return static_cast<int>(i);
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
  // Decoding changes column offsets just like assignment; cached lookups
  // keyed by the old identity must not survive even a partial decode.
  sc.identity_ = Schema::NextLookupIdentity();
  e >> sc.name_ >> sc.columns_;
  return e;
}

}  // namespace tinylamb
